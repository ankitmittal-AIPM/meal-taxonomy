The RASOI dataset contains images of several Indian dishes either standalone or in a combination of multiple dishes served together. The dataset has two folders named "COMPOSITE THALI" and "STANDALONE RECIPE" for these two categories. Each folder contains raw images and its annotation in separate sub-folders. The "RAW IMAGES" folder contains all images and the "ANNOTATION" folder contains one excel consisting of annotations for all images.

For the "COMPOSITE THALI" image annotation, the annotation excel contains three columns - filename, region_shape_attributes, and region_attributes
filename = contains the raw image file name present in respective "RAW IMAGES" sub-folder.
region_shape_attributes = contains four bounding box coordinates for each dish present in the thali image.
region_attributes = The name of the dish enclosed by the bounding box.

Note: The annotation excel may contain multiple entries for each composite thali image if it consists of multiple dishes.

For the "STANDALONE RECIPE" image annotation, the annotation excel contains two columns - Name and Label
Name = contains the image file name present in respective "RAW IMAGES" sub-folder.
Label = The recipe present in the image.

Note that, our standalone recipe images do not contain bounding box annotation.
