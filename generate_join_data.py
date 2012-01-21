import random

# how many items should be generated
AMOUNT = 1000

# create data and write file A (note: AMOUNT*0.7 is used to generate more matches between A and B)
join_a = open('join-data-a.csv', 'w')
data_b = []
for i in range(AMOUNT):
  data = {'i': i, 'join_attr': random.randint(0, AMOUNT*0.7)}
  join_a.write("%(join_attr)s, DataA %(i)d\n" % data)
  data_b.append("%(join_attr)s, DataB %(i)d\n" % data)

# shuffel data B and write it
random.shuffle(data_b)
join_b = open('join-data-b.csv', 'w')
for line in data_b:
  join_b.write(line)
