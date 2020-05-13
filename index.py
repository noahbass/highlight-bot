import streamlink
from PIL import Image
import cv2
import game_screen

# def get_score(image_path: str):
#     with PyTessBaseAPI() as api:
#         api.SetImageFile('example.png')
#         print(api.GetUTF8Text())
#         print(api.AllWordConfidences())


# get_score('stop.jpg')
# print(pytesseract.image_to_string(Image.open('stop.jpg')))
#print(tesserocr.tesseract_version())  # print tesseract-ocr version
#print(tesserocr.get_languages())  # prints tessdata path and list of available languages

# get_score('example.png')

if __name__ == "__main__":
    streams = streamlink.streams("https://twitch.tv/r9rai")
    
    try:
        stream = streams["720p60"] # default
    except:
        stream = streams["720p"] # backup

    fd = stream.open()
    stream_url = fd.writer.stream.url
    fd.close()

    capture = cv2.VideoCapture(stream_url)
    print('stream url:', stream_url)
    # position = capture.get(0)
    # print('Current position:', position)

    ok, frame = capture.read()
    frame_count = 0
    frame_rate = 120 # how many frames to skip over for each frame captured

    while capture.isOpened():
        try:
            # Capture every 
            capture.set(cv2.CAP_PROP_POS_FRAMES, frame_count) # skip to next frame

            ok, frame = capture.read()
            # print('ok?', ok)
            position = capture.get(0)
            print('Current position:', position)

            if ok == True:
                cv2_im = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                image = Image.fromarray(cv2_im)
                frame_count += frame_rate

                # image.show()
                print(game_screen.process_image(image))
            else:
                break
        except KeyboardInterrupt:
            break
    
    cv2.destroyAllWindows()
    capture.release()

    # for i, ts in enumerate(tsFiles):
    #     vid = 'vid{}.ts'.format(i)
    #     process = subprocess.run(['curl', ts, '-o', vid])
