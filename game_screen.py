import time
import re
from tesserocr import PyTessBaseAPI, RIL, PSM, OEM
import tesserocr
from PIL import Image, ImageOps
import pre_process


RESIZE_FACTOR = 4


def get_clock(clock_image):
    clock = clock_image.crop((0, 0, 65*RESIZE_FACTOR, 25*RESIZE_FACTOR))
    clock = pre_process.invert_if_neccessary(clock)
    # clock.show()

    with PyTessBaseAPI() as api:
        api.SetImage(clock)
        # api.SetRectangle(0, 0, 83*RESIZE_FACTOR, 30*RESIZE_FACTOR) # Pillow coordinates (ex: image.crop((60, 35, 350, 65)))
        
        text = api.GetUTF8Text().replace('\n', '').strip(' ')
        confidence = api.AllWordConfidences()

        total_seconds = 0

        if ':' in text:
            try:
                game_minutes, game_seconds = text.split(':')
                game_minutes, game_seconds = int(game_minutes), int(game_seconds)
                total_seconds = (game_minutes * 60) + game_seconds

                return text, total_seconds
            except (ValueError, TypeError):
                return None, None

            # result['clock'] = text
            # result['gametime'] = total_seconds # in seconds
            # print(f'"{text}" (with confidence {str(confidence)})')
        
        return None, None

def get_score(score_image):
    score = score_image.crop((150*RESIZE_FACTOR, 0, 210*RESIZE_FACTOR, 25*RESIZE_FACTOR))
    score = pre_process.invert_if_neccessary(score)
    score.show()

    with PyTessBaseAPI(psm=PSM.SINGLE_LINE, oem=OEM.DEFAULT) as api:
        api.SetImage(score)
        text = api.GetUTF8Text().replace('\n', '').strip(' ')
        confidence = api.AllWordConfidences()

        total_seconds = 0
        home_score, away_score = None, None

        # Only good if score matches format "x-y" where x and y are numbers
        if text is not None \
           and '-' in text \
           and re.match("^[0-9]+(-[0-9]+)$", text) is not None:
            home_score, away_score = text.split('-')
            home_score = int(home_score)
            away_score = int(away_score)

        return home_score, away_score

def get_home_team_name(home_image):
    home = home_image.crop((86*RESIZE_FACTOR, 0, 140*RESIZE_FACTOR, 25*RESIZE_FACTOR))
    home = pre_process.invert_if_neccessary(home)
    home.show()

    with PyTessBaseAPI() as api:
        api.SetImage(home)
        text = api.GetUTF8Text().replace('\n', '').strip(' ')
        # confidence = api.AllWordConfidences()

        # Good only if the name is exactly 3 uppercase characters
        if re.match("^[A-Za-z1-9]{3}$", text) is None:
            return None # team name wasn't good

        return text

def get_away_team_name(away_image):
    away = away_image.crop((210*RESIZE_FACTOR, 0, 270*RESIZE_FACTOR, 25*RESIZE_FACTOR))
    away = pre_process.invert_if_neccessary(away)
    # away.show()

    with PyTessBaseAPI() as api:
        api.SetImage(away)
        text = api.GetUTF8Text().replace('\n', '').strip(' ')
        # confidence = api.AllWordConfidences()
        
        # Good only if the name is exactly 3 uppercase characters
        if re.match("^[A-Za-z1-9]{3}$", text) is None:
            return None # team name wasn't good

        return text


# Process a Pillow image object to detect score
def process_image(image) -> dict:
    result = dict()
    result['timestamp'] = int(time.time()) # unix timestamp

    # Pre-process: Get just the scoreboard portion of the screen
    image_scaled = pre_process.pre_process(image, (70, 38, 350, 62), RESIZE_FACTOR)
    image_scaled.show()
    clock_text, total_seconds = get_clock(image_scaled)
    home_score, away_score = get_score(image_scaled)
    home_name = get_home_team_name(image_scaled)
    away_name = get_away_team_name(image_scaled)

    result['clock'] = clock_text
    result['gametime'] = total_seconds # in seconds
    result['home_score'] = home_score
    result['away_score'] = away_score
    result['home_name'] = home_name
    result['away_name'] = away_name
    
    return result
