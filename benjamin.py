# -*- coding: utf-8 -*-

import boto3
import csv
import json
import pprint

from helpers import *

# Account type matters only slightly for reserved instances in that
# capacity is not guaranteed in your VPC if you have a classic RI that applies
# and visa-versa.


# TODO: Search for All-upfront amazing deals - like one cent, or one dollar and buy those.

def go():
    client = boto3.client('ec2', 'us-east-1')
    ec2 = boto3.resource('ec2', 'us-east-1')

    print('determining account type...')
    account_type = determine_account_type(client)

    print('getting my reserved instances...')
    reservations = get_ris(client)

    print('getting running instances...')
    instances, instance_class_counts = get_instances(account_type, client, ec2)

    print('getting recommendations...')
    make_recommendations(reservations, instances, instance_class_counts, client,
                         ec2, account_type)


def make_recommendations(reservations, instances, instance_class_counts, client,
                         ec2, account_type):
    unreserved_instances = list(instances)

    print()
    print('Instance class counts --------------------------------------------')
    for klass in sorted(instance_class_counts, key=instance_class_counts.get,
                        reverse=True):
        print(str(instance_class_counts[klass]) + str(klass) + ' ')

    print()
    print('Utilized reserved instances --------------------------------------')
    for instance in instances:
        match = match_reservations(instance, instance_class_counts,
                                   reservations, unreserved_instances)
        if match:
            reservation, rtype, rzone, rplatform, iid = match
            print(str((rtype, rzone, rplatform)) +
                  ' reservation is utilized by instance: ' + iid + ' ' +
                  str(instance_name(instance)) + ' security-groups: ' +
                  str(get_groups(instance)) + ' ' +
                  instance.get('VpcId', 'non-vpc'))

    print()
    print('Unreserved instances ----------------------------------------------')
    writer = csv.writer(open("unreserved_instances.csv", 'w'))
    for instance in sorted(unreserved_instances,
                           key=lambda x: x['InstanceType']):
        groups = get_groups(instance)
        unreserved_instances_out = [
            instance['InstanceId'],
            instance['InstanceType'],
            instance['Placement']['AvailabilityZone'],
            instance['bj_Platform'],
            instance_name(instance),
            'security-groups:' + str(groups),
            instance.get('VpcId', 'non-vpc')
        ]
        print(*unreserved_instances_out)
        writer.writerow(unreserved_instances_out)

    print()
    print('Unused reservations -----------------------------------------------')
    unused_reservations = get_unused_reservations(reservations)
    for reservation in unused_reservations:
        r_type = reservation['InstanceType']
        r_zone = reservation['AvailabilityZone']
        r_platform = reservation['ProductDescription']
        diff = reservation['UnusedInstanceCount']
        print(str((r_type, r_zone, r_platform)) + ' has ' + str(diff) +
              ' unused instance(s)!')

    print()
    print('Naive recommended reservation changes -----------------------------')
    az_only_different = []
    family_and_zone_different = []
    family_only_different = []
    for reservation in unused_reservations:
        for instance in unreserved_instances:
            have_same_type = same_instance_type(instance, reservation)
            have_same_platform = same_platform(instance, reservation)
            have_same_az = same_availability_zone(instance, reservation)
            have_same_family = same_family(instance, reservation)
            r_id = reservation['ReservedInstancesId']
            if have_same_type and have_same_platform and not have_same_az:
                # Only zone is different
                az_only_different.append(reservation)
                print('Change reservation: ' + r_id + ' ZONE CHANGE ONLY -'
                    ' to availability zone ' +
                    instance['Placement']['AvailabilityZone'] +
                    ' to utilize this reservation')
                # TODO: Change zone, no-brainer, and remove unreserved instance
            elif have_same_family and not have_same_type and have_same_platform and not have_same_az:
                family_and_zone_different.append(reservation)
                print('Change reservation: ' + r_id + ' to availability zone ' +
                      instance['Placement']['AvailabilityZone'] + ' and instance type from ' +
                      reservation['InstanceType'] + ' to ' +
                      instance['InstanceType'] + ' to utilize this reservation')
                check_reservation_sizing(instance, reservation)
            elif have_same_family and have_same_platform and have_same_az:
                # Just need to change type
                family_only_different.append(reservation)
                print('Change reservation: ' + r_id + ' instance type from ' +
                      reservation['InstanceType'] + ' to ' +
                      instance['InstanceType'] + ' to utilize this reservation')
                check_reservation_sizing(instance, reservation)

    print()
    print('Recommended reservation changes ------------------------------------------')
    type_changes = pack_reservations(unused_reservations, unreserved_instances)
    for suggestion in type_changes:
        print('Change reservation ' + suggestion['reservation']['ReservedInstancesId'])
        print('  current instance type: ' + suggestion['reservation_type'])
        print('  suggest instance type: ' + suggestion['instance_type'])
        if suggestion['same_zone']:
            print('  both already in same zone: ' + suggestion['instance_zone'])
        else:
            print('  plus change zone from: ' + suggestion['instance_zone'] +
                  ' to ' + suggestion['reservation_zone'])
        print('  new utilization: ' + str(suggestion['utilization']))
        print('  instances: ')
        for instance in suggestion['instances']:
            print('    ' + str(instance_name(instance)) + ' security-groups: ' +
                  str(get_groups(instance)) + ' ' +
                  instance.get('VpcId', 'non-vpc'))

    # TODO: See if we can split reservation into two different types

    print()
    print('Recommended reserved instances ------------------------------------')
    suggested_reservations = get_suggested_reservations(unreserved_instances,
                                                        client, account_type)
    for instance, offerings in suggested_reservations:
        print('  \nFor instance-id: ' + instance['InstanceId'] + ' ' +
              str(instance_name(instance)) + ' security-groups: ' +
              str(get_groups(instance)) + ' ' + instance.get('VpcId', 'non-vpc'))
        for i, offering in enumerate(offerings):
            print(str(i) + ') Recommended offering:')
            print('  instance type:             ' + offering['InstanceType'])
            if offering['Marketplace']:
                print('  savings over standard:     ' + str(offering['Savings']))
                print('  comparable total cost:     ' + str(offering['ComparableTotalCost']))
                print('  standard total cost:       ' + str(offering['StdTotalCost']))
                print('  effective hourly:          ' + str(offering['EffectiveHourly']))
                print('  standard effective hourly: ' + str(offering['StdEffectiveHourly']))
                print('  comparable upfront:        ' + str(offering['ComparableUpfront']))
            print('  3rd-party:                 ' + str(offering['Marketplace']))
            print('  zone:                      ' + offering['AvailabilityZone'])
            print('  effective hourly:          ' + str(offering['EffectiveHourly']))
            print('  upfront:                   ' + str(offering['FixedPrice']))
            print('  total cost:                ' + str(offering['TotalCost']))
            print('  years:                     ' + str(offering['Hours'] / HOURS_IN_YEAR))
            print('  id:                        ' + offering['ReservedInstancesOfferingId'])
            print('  platform                   ' + offering['ProductDescription'])
            print('  type                       ' + offering['OfferingType'])
            print('  amazing deal               ' + str(offering.get('AmazingDeal', False)))
        print('What reservation do you want? Press enter to skip: ')
        valid = False
        while not valid:
            choice = input()
            if choice.isdigit() or choice == '':
                valid = True
        if choice.isdigit():
            #  Buy reservation
            reservation = offerings[int(choice)]
            reservation_id = reservation['ReservedInstancesOfferingId']
            amount = reservation['FixedPrice']
            count = 1
            print('Are you sure you want to buy ' + reservation_id +
                  ' for $' + str(amount) + '? (y/n) ')
            confirm = input()
            if confirm == 'y':
                try:
                    purchase_reserved_instance(reservation_id, client, count, amount)
                except Exception as e:
                    print('Problem reserving instance, exception below :\n'
                          + str(e))
            else:
                print('Skipping')
        else:
            print('Skipping')

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


def get_groups(instance):
    groups = [group['GroupName'] for group in instance['SecurityGroups']]
    return groups


if __name__ == '__main__':
    go()
