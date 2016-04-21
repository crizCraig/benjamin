# -*- coding: utf-8 -*-
from collections import OrderedDict
import sys

import botocore

ACCOUNT_TYPE_VPC_DEFAULT = 'VPC-default'
ACCOUNT_TYPE_EC2_CLASSIC = 'EC2-classic'
HOURS_IN_YEAR = 24.0 * 365.0
HOURS_IN_3_YEARS = HOURS_IN_YEAR * 3.0
SECONDS_IN_HOUR = 3600.0
RESERVATION_YEAR_PREFERENCE = 1;
RESERVATION_PREFERENCES = {
    'Seconds': 60 * 60 * HOURS_IN_YEAR * RESERVATION_YEAR_PREFERENCE,
    'Hours': RESERVATION_YEAR_PREFERENCE * HOURS_IN_YEAR,
    'OfferingType': 'Partial Upfront',
    'ClassesToIgnore': ['c3.8xlarge']
}
RESERVATION_MAP = {
    'micro':     0.5,
    'small':     1,
    'medium':    2,
    'large':     4,
    'xlarge':    8,
    '2xlarge':  16,
    '4xlarge':  32,
    '8xlarge':  64,
    '10xlarge': 80
}


def same_family(instance, reservation):
    return instance['InstanceType'].split('.')[0] == \
           reservation['InstanceType'].split('.')[0]


def same_availability_zone(instance, reservation):
    return reservation['AvailabilityZone'] == instance['Placement'][
        'AvailabilityZone']


def same_instance_type(instance, reservation):
    return instance['InstanceType'] == reservation['InstanceType']


def same_platform(instance, reservation):
    return get_account_agnostic_platform(instance['bj_Platform']).lower() == \
           get_account_agnostic_platform(
               reservation['ProductDescription']).lower()


def get_account_agnostic_platform(platform):
    return platform.replace(' (Amazon VPC)', '')


def get_availability_zone(ins_or_res):
    if 'AvailabilityZone' in ins_or_res:
        return ins_or_res['AvailabilityZone']
    elif 'Placement' in ins_or_res:
        return ins_or_res['Placement']['AvailabilityZone']


def match_reservations(instance, instance_class_counts, reservations,
                       unreserved_instances):
    for reservation in reservations:
        # if reservation matches and not used, mark used and print utilized
        rcount = reservation['InstanceCount']
        if 'UsedInstanceCount' not in reservation:
            reservation['UsedInstanceCount'] = 0
        rused_count = reservation['UsedInstanceCount']
        if rcount != rused_count:
            rtype = reservation['InstanceType']
            rzone = reservation['AvailabilityZone']
            rplatform = get_account_agnostic_platform(
                reservation['ProductDescription'])
            itype = get_instance_type(instance)
            izone = get_availability_zone(instance)
            iid = instance['InstanceId']
            iplatform = get_account_agnostic_platform(instance['bj_Platform'])
            if itype == rtype and izone == rzone and \
                            iplatform.lower() == rplatform.lower():
                reservation['UsedInstanceCount'] += 1
                if instance in unreserved_instances:
                    unreserved_instances.remove(instance)
                return reservation, rtype, rzone, rplatform, iid
    return None


def instance_name(instance):
    if 'Tags' in instance:
        for tag in instance['Tags']:
            if tag['Key'] == 'Name':
                return tag['Value']


def get_ris(client):
    reserved_instances = client.describe_reserved_instances(
        #DryRun=True|False,
        #ReservedInstancesIds=[
        #    'string',#
        #],
        # Filters=[
        #     {
        #         'Name': 'string',
        #         'Values': [
        #             'string',
        #         ]
        #     },
        # ],
        #OfferingType='Heavy Utilization'|'Medium Utilization'|'Light Utilization'|'No Upfront'|'Partial Upfront'|'All Upfront'
    )

    # Get reserved instances that are not retired
    ret = []
    for reservation in reserved_instances['ReservedInstances']:
        reservation_state = reservation['State']
        if reservation_state == 'active' or \
                reservation_state == 'payment-pending':
            ret.append(reservation)

    return ret


def get_instances(account_type, client, ec2):
    max_results = 1000
    instances = client.describe_instances(
        #DryRun=True|False,
        #InstanceIds=[
        #    'string',
        #],
        #Filters=[
        #    {
        #        'Name': 'string',
        #        'Values': [
        #            'string',
        #        ]
        #    },
        #],
        #NextToken='string',
        MaxResults=max_results
    )
    if len(instances) == max_results:
        raise Exception('Need to implement instance paging')

    running_instances = []
    instance_class_counts = {}
    cached_images = {}
    for instance_data in instances['Reservations']:
        info = instance_data['Instances']
        for instance in info:
            i_type = instance['InstanceType']
            zone = instance['Placement']['AvailabilityZone']
            state = instance['State']['Name']
            if state != 'running':
                continue
            platform = get_platform(instance, account_type, cached_images, ec2)
            instance['bj_Platform'] = platform
            if not platform:
                continue
            running_instances.append(instance_data['Instances'][0])
            instance_class = (i_type, zone, platform)
            if instance_class in instance_class_counts:
                instance_class_counts[instance_class] += 1
            else:
                instance_class_counts[instance_class] = 1
    return running_instances, instance_class_counts


def get_platform(instance, account_type, cached_images, ec2):
    if 'Platform' in instance:
        platform = instance['Platform']
    else:
        if instance['ImageId'] in cached_images:
            image = cached_images[instance['ImageId']]
        else:
            image = ec2.Image(instance['ImageId'])
            cached_images[instance['ImageId']] = image
        image_name = image.name.lower()
        if 'red' in image_name or 'windows' in image_name or \
                'suse' in image_name:
            print('This is unexpected, instance: ' +
                  instance['InstanceId'] +
                  ' has image ' + image_name +
                  ' that may be in a platform that '
                  'is not Linux/Unix. Skipping instance.')
            return None
        else:
            platform = 'Linux/UNIX'

    # if account_type == ACCOUNT_TYPE_EC2_CLASSIC and 'VpcId' in instance:
    #     platform += ' (Amazon VPC)'

    return platform


def get_offerings(i_type, zone, platform, client, account_type):
    def make_request(mod_platform):
        offerings = client.describe_reserved_instances_offerings(
            # DryRun=True|False,
            # ReservedInstancesOfferingIds=[
            #    'string',
            # ],
            InstanceType=i_type,
            AvailabilityZone=zone,
            ProductDescription=mod_platform,
            # Filters=[
            #    {
            #        'Name': 'string',
            #        'Values': [
            #            'string',
            #        ]
            #    },
            # ],
            # InstanceTenancy='default'|'dedicated'|'host',
            # OfferingType='Heavy Utilization'|'Medium Utilization'|'Light Utilization'|'No Upfront'|'Partial Upfront'|'All Upfront',
            # NextToken='string',
            MaxResults=1000,
            IncludeMarketplace=True,
            # MinDuration=123,
            # MaxDuration=123,
            MaxInstanceCount=1000)
        return offerings['ReservedInstancesOfferings']

    ret = make_request(platform)
    if account_type == ACCOUNT_TYPE_EC2_CLASSIC:
        vpc_offerings = make_request(platform + ' (Amazon VPC)')
        ret += vpc_offerings

    return ret


def get_default_offerings(client):
    offerings = client.describe_reserved_instances_offerings(
        MaxResults=1000,
        IncludeMarketplace=True,
        MaxInstanceCount=1000)
    return offerings


def determine_account_type(client):
    default_offerings = get_default_offerings(client)
    for offering in default_offerings['ReservedInstancesOfferings']:
        if 'VPC' in offering['ProductDescription']:
            return ACCOUNT_TYPE_EC2_CLASSIC
    return ACCOUNT_TYPE_VPC_DEFAULT


def get_instance_type(ins_or_res):
    return ins_or_res['InstanceType']


def get_instance_family(instance):
    return instance['InstanceType'].split('.')[0]


def get_instance_size(instance):
    return RESERVATION_MAP[instance['InstanceType'].split('.')[1]]


def check_reservation_sizing(instance, reservation):
    in_units = get_instance_size(instance)
    re_units = get_instance_size(reservation)
    pass


def pack_reservations(reservations, instances):
    ins_by_class = {}

    # Get instances by type
    for instance in instances:
        ins_type = instance['InstanceType']
        ins_zone = get_availability_zone(instance)
        cls = (ins_type, ins_zone)
        ins_in_class = ins_by_class.get(cls, [])
        ins_in_class.append(instance)
        ins_by_class[cls] = ins_in_class

    suggestions = []

    # Look for instance classes that would fulfill a reservation
    # (i.e. 8 or less m1.small's) and give utilization rate
    # Sort by utilization rate
    for reservation in reservations:
        re_type = get_instance_type(reservation)
        re_family = get_instance_family(reservation)
        for ins_cls in ins_by_class:
            ins_type, ins_zone = ins_cls
            ins_family = ins_type.split('.')[0]
            if ins_family == re_family:
                instances = ins_by_class[ins_cls]
                count = len(instances)
                instance = instances[0]
                ins_size = get_instance_size(instances[0])
                ins_units = count * ins_size
                re_count = reservation['InstanceCount']
                re_used_count = reservation['UsedInstanceCount']
                re_unused_count = re_count - re_used_count
                re_total_count = re_unused_count + re_used_count
                re_used_fraction = re_used_count / re_total_count
                re_units = get_instance_size(reservation) * re_total_count
                re_zone = get_availability_zone(reservation)
                ins_zone = get_availability_zone(instance)
                same_zone = re_zone == ins_zone
                utilization = float(ins_units) / float(re_units)

                if abs(1.0 - utilization) < 0.25:
                    suggestions.append(OrderedDict({
                        'reservation': reservation,
                        'instances': instances,
                        'utilization': utilization,
                        'instance_zone': re_zone,
                        'reservation_zone': ins_zone,
                        'same_zone': same_zone,
                        'instance_count': count,
                        'instance_units': ins_units,
                        'reserved_units': re_units,
                        'instance_type': ins_type,
                        'reservation_type': re_type,
                    }))
        # TODO: Suggest availability zone.
        # TODO: Utilization far from 1.0 could give better results buying on third-party.
        # TODO: Look for opportunities to combine reserved instances to cover current large instances.
        # TODO: Incorporate fact that reservations can be changed to multiple smaller reservations of different classes.

    if len(suggestions) > 0:
        suggestions = sorted(suggestions, key=lambda x: -x['utilization'])
    return suggestions


def analyze_offerings(offerings):
    # x Give total price after term = duration / 60 * hourly + fixed_price
    # x Advise about declining AWS costs.
    # x Calculate effective rate = Total dollars / total hours
    # x Caclulate effective 1 year if duration less than 1 year = duration / 1 year * fixedPrice
    # x Caclulate effective 3 years if duration greater than 1 year and less than 3 years = duration / 3 years * fixedPrice
    # x Return reservations that are less than preferred duration if effective rate and fixed price less and effective upfront price within 10% - Prob 3rd party
    # x If effective rate / max-effective-rate less than .20, regardless of duration - highlight as AMAZING DEAL
    # - Confirm you want to reserve

    std_offerings = {}

    ret = []
    for offering in offerings:
        recurring = offering['RecurringCharges']
        if len(recurring) == 0:
            offering['RecurringCharges'] = [{'Frequency': 'Hourly', 'Amount': 0.0}]
            recurring = offering['RecurringCharges']

        if len(recurring) != 1:
            raise Exception('Unexpected recurring charges format')
        elif recurring[0]['Frequency'] != 'Hourly':
            raise Exception('Non-hourly recurring frequency not supported')
        else:
            min_effective_hourly, max_effective_hourly = \
                populate_calculated_offering_fields(offering)

    for offering in offerings:
        if not offering['Marketplace']:  # Zone assumed to be the same
            std_offerings[(offering['Duration'], offering['OfferingType'])] = offering

    for offering in offerings:
        seconds = offering['Duration']
        hours = offering['Hours']
        pref_hours = RESERVATION_PREFERENCES['Hours']
        pref_ofr_type =  RESERVATION_PREFERENCES['OfferingType']
        ofr_type = offering['OfferingType']
        upfront = offering['FixedPrice']
        effective_hourly = offering['EffectiveHourly']
        comparable_upfront = offering['ComparableUpfront']
        comparable_total_cost = offering['ComparableTotalCost']
        std_offer = std_offerings.get((offering['ComparableDuration'],
                                       ofr_type), None)
        if not std_offer:
            print('Skipping light, medium, heavy 3rd-party offering.')
        else:
            # Return reservations that are less than preferred duration if
            # effective rate and fixed price less and effective upfront price
            # within 10%
            std_upfront = std_offer['FixedPrice']
            std_effective_hourly = std_offer['EffectiveHourly']
            std_total_cost = std_offer['TotalCost']
            total_cost = offering['TotalCost']
            fraction_of_comp = offering['FractionOfComparable']
            offering['StdUpfront'] = std_upfront
            offering['StdEffectiveHourly'] = std_effective_hourly
            offering['StdTotalCost'] = std_total_cost
            offering['Savings'] = std_total_cost * fraction_of_comp - total_cost
            if (
                offering['Marketplace'] and
                hours < pref_hours and
                ofr_type == pref_ofr_type and
                upfront < std_upfront and
                comparable_upfront < std_upfront * 1.1 and
                comparable_total_cost <= std_total_cost * 1.1
            ):
                ret.append(offering)

            if comparable_total_cost < (std_total_cost * 0.2):
                offering['AmazingDeal'] = True
                if offering not in ret:
                    ret.append(offering)

    pref_seconds = RESERVATION_PREFERENCES['Seconds']
    pref_type = RESERVATION_PREFERENCES['OfferingType']
    if ret:
        ret = sorted(ret, key=lambda x: x['Savings'], reverse=True)
    ret.append(std_offerings[int(pref_seconds), pref_type])
    return ret


def populate_calculated_offering_fields(offering):
    min_effective_hourly = 0.0
    max_effective_hourly = sys.float_info.min
    hourly = offering['RecurringCharges'][0]['Amount']
    hours = offering['Duration'] / SECONDS_IN_HOUR
    upfront = offering['FixedPrice']
    total_cost = upfront + hours * hourly
    effective_hourly = total_cost / hours
    if hours <= HOURS_IN_YEAR:
        std_mult = HOURS_IN_YEAR / float(hours)
        comparable_duration = HOURS_IN_YEAR * SECONDS_IN_HOUR
    elif hours <= HOURS_IN_3_YEARS:
        std_mult = HOURS_IN_3_YEARS / float(hours)
        comparable_duration = HOURS_IN_3_YEARS * SECONDS_IN_HOUR
    else:
        raise Exception('Only 1 and 3 year reservations supported')
    comparable_upfront = std_mult * upfront
    comparable_total_cost = std_mult * total_cost
    if effective_hourly < min_effective_hourly:
        min_effective_hourly = effective_hourly
    if effective_hourly > max_effective_hourly:
        max_effective_hourly = effective_hourly
    offering['Hours'] = hours
    offering['TotalCost'] = total_cost
    offering['ComparableTotalCost'] = comparable_total_cost
    offering['EffectiveHourly'] = effective_hourly
    offering['ComparableUpfront'] = comparable_upfront
    offering['ComparableDuration'] = comparable_duration
    offering['FractionOfComparable'] = 1.0 / std_mult
    return min_effective_hourly, max_effective_hourly


def get_suggested_reservations(instances, client, account_type):
    ret = []
    for instance in instances:
        classes_to_ignore = RESERVATION_PREFERENCES['ClassesToIgnore']
        if instance['InstanceType'] in classes_to_ignore:
            continue
        i_type = instance['InstanceType']
        zone = instance['Placement']['AvailabilityZone']
        offerings = get_offerings(i_type, zone, instance['bj_Platform'], client,
                                  account_type)
        good_offerings = analyze_offerings(offerings)
        ret.append((instance, good_offerings))
    ret = sorted(ret, key=lambda x: x[1][0]['TotalCost'], reverse=True)
    return ret


def purchase_reserved_instance(offer_id, client, count, amount):
    response = client.purchase_reserved_instances_offering(
        # DryRun=True|False,
        ReservedInstancesOfferingId=offer_id,
        InstanceCount=count,
        LimitPrice={
            'Amount': amount,
            'CurrencyCode': 'USD'
        }
    )

    return response


def get_unused_reservations(reservations):
    unused_reservations = []
    for reservation in reservations:
        r_count = reservation['InstanceCount']
        r_used_count = reservation['UsedInstanceCount']
        diff = r_count - r_used_count
        reservation['UnusedInstanceCount'] = diff
        if r_used_count != r_count:
            unused_reservations.append(reservation)
    return unused_reservations

