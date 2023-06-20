# import the required packages
import argparse

# we frist create the ArgumentsParser object
# The craeted object 'parser' will have the necessary information
# to parse the command-line arguments into data types
parser = argparse.ArgumentParser()

#we add a positional argument using add_argument() including a help
parser.add_argument('first_argument', help='this is the string text in connection with first_argument')

# The information about program arguments is stored in 'parser' and used when parse_args() is called
# ArgumentParser parses arguments through the parse_args method:
args = parser.parse_args()

# we get and print the first argument of this script
print(args.first_argument)
