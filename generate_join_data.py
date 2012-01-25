import random

# how many items should be generated
AMOUNT = 1000

# create data and write file A (note: AMOUNT*0.7 is used to generate more matches between A and B)
join_a = open('join-data-a.csv', 'w')
data_b = []
for i in range(AMOUNT):
    random_key = random.randint(0, AMOUNT*0.7)
    data = {'i': i, 'join_attr': random_key}
    join_a.write("%(join_attr)s, DataA %(i)d\n" % data)
    data_b.append("%(join_attr)s, DataB %(i)d\n" % data)

# shuffel data B and write it
random.shuffle(data_b)
join_b = open('join-data-b.csv', 'w')
for line in data_b:
    join_b.write(line)

# create a relation that contains keys from 
base = open('join-data-base.csv', 'w')
for key in random.sample(list(range(AMOUNT)), int(AMOUNT*0.8)):
    base.write("%s, Data\n" % key)
