import boto3
import random
import names 

questnames = {
    "30 minutes of exercise":"1001",
    "Walk 10,000 steps":"1002",
    "Walk 100,000":"1003",
    "Run 10k":"1004",
    "Run 5k":"1005"
}

tableidentifier='questmember'

def write_quest(dyn_resource=None):
    for key in questnames:
            if dyn_resource is None:
                dyn_resource = boto3.resource('dynamodb')
            table = dyn_resource.Table(tableidentifier)
            questid='Q#'+questnames[key]
            table.put_item(Item={
                'PK': questid,
                'SK': 'METADATA',
                'Quest':key
            })
            print(f"Put item ({questid}, {key}) succeeded.")

def write_data_table(key_count, item_size, dyn_resource=None):
    """
    Writes test data to the demonstration table.

    :param key_count: The number of partition and sort keys to use to populate the
                      table. The total number of items is key_count * key_count.
    :param item_size: The size of non-key data for each test item.
    :param dyn_resource: Either a Boto3 or DAX resource.
    """
    if dyn_resource is None:
        dyn_resource = boto3.resource('dynamodb')

    table = dyn_resource.Table(tableidentifier)
    some_data = 'X' * item_size
    for partition_key in range(1, key_count + 1):
        memberid='M#'+str(random.randint(1000,10000))
        firstname=names.get_first_name()
        lastname=names.get_last_name()
        emailid=firstname+"."+lastname+"@dummyemail.com"
        numberofquest=random.randint(1,5)
        try:
            table.put_item(Item={
                'PK': memberid,
                'SK': 'METADATA',
                'firstname':firstname,
                'lastname':lastname,
                'emailid':emailid,
                'some_data': some_data
            },ConditionExpression='attribute_not_exists(PK)')
        except Exception:
            pass
        
        for quest_loop in range(1,numberofquest ):
            questname=random.choice(list(questnames.keys()))
            questid='Q#'+questnames[questname]
            table.put_item(Item={
                'PK': memberid,
                'SK': questid,
                'dollars_earned': random.randint(1,10)#round(random.uniform(0.0, 9.0),2)
            })
            print(f"Put item ({memberid}, {questid}) succeeded.")

if __name__ == '__main__':
    write_key_count = 100
    write_item_size = 1000
    print(f"Writing {write_key_count} items to the table. "
          f"Each item is {write_item_size} characters.")
    write_quest()
    write_data_table(write_key_count, write_item_size)
