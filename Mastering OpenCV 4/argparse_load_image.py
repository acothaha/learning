# Import the required packages
import argparse
import cv2

# We first create the ArgumentParser object
# The created obcjet 'parser' will have the necessary information
# to parse the command-line arguments into data types
parser = argparse.ArgumentParser()

# We add 'path_image' argument using add_argument() including help
# The type of this argument is string (by default)
parser.add_argument('path_image', help='path to input image to be displayed')

# The information about program argument is stored in 'parser'
# Then, it is used when the parser calls parse_args()
# ArgumentParser parses arguments through the parse_args() method
args = parser.parse_args()

# We can now load the input image from disk
image = cv2.imread(args.path_image)

# Parse the argument and store it in a dictionary
arg = vars(parser.parse_args())

# Now, we can also load the input image from disk using argsL
image2 = cv2.imread(args['path_image'])

# show the loaded image
cv2.imshow('loaded image', image)
cv2.imshow('loaded image2', image2)

# Wait until a key is pressed:
cv2.waitKey(0)

# Destroy all windows
cv2.destroyAllWindows()