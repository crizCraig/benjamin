# -*- coding: utf-8 -*-

import boto3
import psycopg2

client = boto3.client('ec2', 'us-east-1')
ec2 = boto3.resource('ec2', 'us-east-1')

ACCOUNT_TYPE_VPC_DEFAULT = 'VPC-default'
ACCOUNT_TYPE_EC2_CLASSIC = 'EC2-classic'

# Account type matters only slightly for reserved instances in that
# capacity is not guaranteed in your VPC if you have a classic RI that applies
# and visa-versa.


def go():
    print('determining account type...')
    account_type = determine_account_type()
    print('done determining account type')

    print('getting my reserved instances...')
    reservations = get_ris()
    print('done getting my reserved instances...')

    print('getting running instances...')
    instances, instance_class_counts = get_instances(account_type)
    print('done getting running instances')

    print('getting recommendations...')
    make_recommendations(reservations, instances, instance_class_counts)


def make_recommendations(reservations, instances, instance_class_counts):
    unreserved_instances = list(instances)

    print()
    print('Utilized reserved instances --------------------------------------')
    for instance in instances:
        find_matching_reservartion(instance, instance_class_counts,
                                   reservations, unreserved_instances)

    print()
    print('Unreserved instances ----------------------------------------------')
    for instance in unreserved_instances:
        print(instance['InstanceId'],
              instance['InstanceType'],
              instance['Placement']['AvailabilityZone'],
              instance['bj_Platform'],
              instance_name(instance))

    print()
    print('Unused reservations -----------------------------------------------')
    for reservation in reservations:
        r_count = reservation['InstanceCount']
        if reservation['InstanceCount'] > 0:
            r_type = reservation['InstanceType']
            r_zone = reservation['AvailabilityZone']
            r_platform = reservation['ProductDescription']
            print(str((r_type, r_zone, r_platform)) + ' has ' + str(r_count) +
                  ' unused instance(s)!')

    print()
    print('Recommended reserved instances ------------------------------------')
    for instance in unreserved_instances:
        i_type = instance['InstanceType']
        zone = instance['Placement']['AvailabilityZone']
        response = client.describe_images(ImageIds=[instance['ImageId']])
        image = ec2.Image('ffc81394')
        offerings = get_offerings(i_type, zone, instance['bj_Platform'])

        pass

        # See what reservations are available, especially third-party, that you would need
        # - Allow non-VPC instances to be reserved by VPC reservations?
        # - Notify about EBS encryption ability of c3+, m3+, r3+
        # - Notify about HVM / PV compatibility
        # - Notify about cost effectiveness of later class c4/m4
        # - Advise changing availibility zone
        # - Advise changing between EC2-VPC and Classic
        # x make sure you select the right type (Linux or Windows)
        # x Check if you are in EC2 classic by seeing if any reserved instance has VPC in its name.
        # x Add VPC to the RI type tuple - non-EC2 classic will all get vpc for that part of the tuple
        # x check availability zone
        # x check VPC only if you just want VPC
        # x make sure instance is running

        # Send email asking if you want to purchase the instance with the months left and months to break even.
        # Allow sending email back with number of option you wish to buy
    # TODO: Recommend instance reservations that can be changed, make sure you cancel current listings.


def find_matching_reservartion(instance, instance_class_counts, reservations,
                               unreserved_instances):
    for reservation in reservations:
        # if reservation matches and not used, mark used and print utilized
        r_count = reservation['InstanceCount']
        if r_count != 0:
            r_type = reservation['InstanceType']
            r_zone = reservation['AvailabilityZone']
            r_platform = reservation['ProductDescription']
            if (r_type, r_zone, r_platform) in instance_class_counts:
                print(str((r_type, r_zone, r_platform)) +
                      ' reservation is utilized')
                reservation['InstanceCount'] -= 1
                if instance in unreserved_instances:
                    unreserved_instances.remove(instance)
                return reservation
    return None


def instance_name(instance):
    if 'Tags' in instance:
        for tag in instance['Tags']:
            if tag['Key'] == 'Name':
                return tag['Value']


def get_ris():
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
    active_ris = []
    for reservation in reserved_instances['ReservedInstances']:
        reservation_state = reservation['State']
        if reservation_state == 'active':
            active_ris.append(reservation)

    return active_ris


def get_instances(account_type):
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
        #MaxResults=123
    )
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
            platform = get_platform(instance, account_type, cached_images)
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


def get_platform(instance, account_type, cached_images):
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


def get_offerings(i_type, zone, platform):
    offerings = client.describe_reserved_instances_offerings(
        # DryRun=True|False,
        # ReservedInstancesOfferingIds=[
        #    'string',
        # ],
        InstanceType=i_type,
        AvailabilityZone=zone,
        ProductDescription=platform,
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
    return offerings


def get_default_offerings():
    offerings = client.describe_reserved_instances_offerings(
        MaxResults=1000,
        IncludeMarketplace=True,
        MaxInstanceCount=1000)
    return offerings


def determine_account_type():
    default_offerings = get_default_offerings()
    for offering in default_offerings['ReservedInstancesOfferings']:
        if 'VPC' in offering['ProductDescription']:
            return ACCOUNT_TYPE_EC2_CLASSIC
    return ACCOUNT_TYPE_VPC_DEFAULT

if __name__ == '__main__':
    go()
