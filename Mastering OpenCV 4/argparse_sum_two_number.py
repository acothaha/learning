# import the required packages
import argparse

# we frist create the ArgumentsParser object
# The craeted object 'parser' will have the necessary information
# to parse the command-line arguments into data types
parser = argparse.ArgumentParser()

# we add 'first_number' argument using add_argumet() including a help. the type of this argument is int
parser.add_argument('first_number', help='first number to be added', type=int)

# we add 'second_number' argument using add_argumet() including a help. the type of this argument is int
parser.add_argument('second_number', help='second number to be added', type=int)

# The information about program arguments is stored in 'parser' and used when parse_args() is called
# ArgumentParser parses arguments through the parse_args method:
args = parser.parse_args()
print('args: "{}"'.format(args))

print('the sum is: "{}"'.format(args.first_number + args.second_number))

# additionally, the arguments can be stored in a dictionary calling vars()
args_dict = vars(parser.parse_args())

# We print this dictionary:
print('args_dict dictionary: "{}"'.format(args_dict))

# for example, to get the first argument using this dictionary
print('first argument from the dictionary: "{}"'.format(args_dict['first_number']))

