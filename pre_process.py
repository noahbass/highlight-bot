from PIL import Image, ImageOps


# Pre-process a Pillow image
# Convert to grayscale, rescale up by a factor of 3, and use thresholding
def pre_process(image, crop_coordinates, resize_factor):
    # Crop before continuing with pre-processing
    image = crop(image, crop_coordinates)

    # https://tesseract-ocr.github.io/tessdoc/ImproveQuality
    # Convert to grayscale, rescale up by factor of 3, and use thresholding to smooth the grayscaling and remove noise
    image_scaled = image
    image_scaled = image_scaled.convert('L')\
                    .resize([resize_factor * _ for _ in image_scaled.size], Image.BICUBIC)\
                    .point(lambda pixel: pixel > 90 and pixel + 100)

    # Max color should be either 0 (black) or 255 (white) for the grayscale image
    dominate_color = max_color(image_scaled.getcolors())

    if dominate_color < 127:
        # Image (likely) has black background, invert to white background with black text
        image_scaled = ImageOps.invert(image_scaled) # This helps tesseract by making
                                                     # everything black text on a white background
    # image_scaled.show()
    
    return image_scaled


def crop(image, crop_coordinates):
    return image.crop(crop_coordinates)


# Find max frequency in a list of grayscale tuples, return the corresponding color
def max_color(color_frequencies):
    return max(color_frequencies, key=lambda item: item[0])[1]
