from flask import Flask, json
from datetime import datetime
from PIL import Image
from PIL import ImageDraw
import os
import requests
app = Flask(__name__)
URLLine = 'https://notify-api.line.me/api/notify'
keyAPI = "tt8NBFOcaq4_3ye-nzda888QrlqQCer9XXAlemoNX9c="
URLPdpa = "http://pdpa.dwh-journey.arctic.true.th/Decryption/{0}/{1}"
@app.route('/')
def index():
    token='oLckduxfq76HjI9utVsP0Is7185hPhWZ5gMziPiJeNc'
    msg ="xx"
    URL = 'https://notify-api.line.me/api/notify'
    headers = {'Authorization': 'Bearer ' + token}
    payload = {'message': msg}  # ,'stickerPackageId':1,'stickerId':1
    r = requests.post(URL, headers=headers, params=payload)
    return str(r.status_code)

def send_message(token, msg, img=None):
    """Send a LINE Notify message (with or without an image)."""
    headers = {'Authorization': 'Bearer ' + token}
    payload = {'message': msg}#,'stickerPackageId':1,'stickerId':1
    files = {'imageFile': open(img, 'rb')} if img else None
    r = requests.post(URLLine, headers=headers, params=payload, files=files)
    if files:
        files['imageFile'].close()
    return r.status_code

@app.route('/line/message/<token>/<ciphertext>', methods=['GET'])
def send_message(token=None,ciphertext=None):
    msg = ""
    html = requests.get(URLPdpa.format(keyAPI,ciphertext))
    result = dict(json.loads(html.text))
    print(result["key"])
    print(result["value"])
    if result["value_cipher"] != None:
        msg = (result["value_cipher"])

    headers = {'Authorization': 'Bearer ' + token}
    payload = {'message': msg}  # ,'stickerPackageId':1,'stickerId':1
    r = requests.post(URLLine, headers=headers, params=payload)
    return str(r.status_code)

@app.route('/line/image_list/<token>/<ciphertext>', methods=['GET'])
def send_image_list(token=None,ciphertext=None):
    fontRed = (255, 0, 0)
    fontBlack = (0, 0, 0)
    fontGreen = (0, 255, 0)
    fontGray = (128, 128, 128)
    fontYellow = (255, 255, 0)

    rowlast = 575

    rowFirst = 75
    rowSpace = 50

    rowColumn1 = 15
    rowColumn2 = 100
    rowColumn3 = 375
    rowColumn3Right = 425
    rowColumnStatus = 530
    rowColumn4 = 600
    rowColumn5 = 780
    rowColumn6 = 1050
    refreshtimelabel="refresh time :"
    message = ""
    refreshtime = ""

    arrMonitor = []
    arrComma = []
    countrow = 0

    MONITOR_NAME=""
    MONITOR_LOADDATE_VALUE=""
    MONITOR_STATUS_VALUE = ""
    MONITOR_STARTDATE_VALUE = ""
    MONITOR_ENDDATE_VALUE = ""
    msg = ""
    print(URLPdpa.format(keyAPI, ciphertext))

    html = requests.get(URLPdpa.format(keyAPI, ciphertext))
    result = dict(json.loads(html.text))
    print(result["key"])
    print(result["value"])
    if result["value_cipher"] != None:
        msg = (result["value_cipher"])
    filebg = 'notify.png'
    filealert = 'notify_alert.png'
    arrMonitor=(msg).split("*")
    if len(arrMonitor)>0:
        img = Image.open(filebg)
        for item in arrMonitor:
            countrow += 1
            arrComma = item.split(",")
            if len(arrComma)>0: MONITOR_NAME=arrComma[0]
            if len(arrComma)>1: MONITOR_STATUS_VALUE=arrComma[1]
            if len(arrComma)>2: MONITOR_LOADDATE_VALUE=arrComma[2]
            if len(arrComma)>3: MONITOR_STARTDATE_VALUE=arrComma[3]
            if len(arrComma)>4: MONITOR_ENDDATE_VALUE=arrComma[4]

            I1 = ImageDraw.Draw(img)
            I1.text((rowColumn1, rowFirst), str(countrow), fill=(0, 0, 0))

            if (MONITOR_NAME != ""):
                I1.text((rowColumn2, rowFirst), str(MONITOR_NAME), fill=(0, 0, 0))
            if (MONITOR_LOADDATE_VALUE != ""):
                I1.text((rowColumn3, rowFirst), str(MONITOR_LOADDATE_VALUE), fill=(128, 128, 128))
            if (MONITOR_STATUS_VALUE != ""):
                I1.text((rowColumnStatus, rowFirst), str(MONITOR_STATUS_VALUE), fill=(128, 128, 128))
            if (MONITOR_STARTDATE_VALUE != ""):
                I1.text((rowColumn4, rowFirst), str(MONITOR_STARTDATE_VALUE), fill=(128, 128, 128))
            if (MONITOR_ENDDATE_VALUE != ""):
                I1.text((rowColumn5, rowFirst), str(MONITOR_ENDDATE_VALUE), fill=(128, 128, 128))
            if (MONITOR_STATUS_VALUE != "Complete"):
                if (message != ""):
                    message += ","
                message += MONITOR_NAME
            rowFirst += rowSpace

        if (refreshtime != ""):
            I1.text((rowColumn3Right, rowlast), refreshtimelabel, fill=(0, 0, 0))
            I1.text((rowColumn4, rowlast), str(refreshtime), fill=(0, 0, 0))

        img.save(filealert)

    if os.path.exists(filealert):
        if (token != ""):
            formatmessage = "alert time : {0}"
            headers = {'Authorization': 'Bearer ' + token}
            payload = {'message':formatmessage.format(str(datetime.now()))}  # ,'stickerPackageId':1,'stickerId':1
            files = {'imageFile': open(filealert, 'rb')} if img else None
            r = requests.post(URLLine, headers=headers, params=payload, files=files)
            if files:
                files['imageFile'].close()

    return str(r.status_code)


@app.route('/line/image_single/<token>/<ciphertext>', methods=['GET'])
def send_image_single(token=None,  ciphertext=None):
    column20 = 20
    row15 = 15
    row30 = 30
    row60 = 60
    row80 = 80
    row100 = 100

    column40 = 40
    column50 = 50
    column85 = 85
    column100 = 100
    column120 = 120

    row140 = 140
    row160 = 160
    row180 = 180

    fontRed = (255, 0, 0)
    fontBlack = (0, 0, 0)
    fontGreen = (0, 255, 0)
    fontGray = (128, 128, 128)
    fontYellow = (255, 255, 0)

    arrMonitor = []
    notify = "* Warning Delay time 2 Hour"
    process = ""
    time_used = ""
    t0 = ""
    t1 = ""
    msg = ""
    html = requests.get(URLPdpa.format(keyAPI, ciphertext))
    result = dict(json.loads(html.text))
    print(result["key"])
    print(result["value"])
    if result["value_cipher"] != None:
        msg = (result["value_cipher"])
    filebg = 'notify_single.png'
    filealert = 'notify_single_alert.png'

    arrMonitor = (msg).split("*")
    if len(arrMonitor) > 0:
        img = Image.open(filebg)
        if len(arrMonitor) > 0: group = arrMonitor[0]
        if len(arrMonitor) > 1: source = arrMonitor[1]
        if len(arrMonitor) > 2: target = arrMonitor[2]
        if len(arrMonitor) > 3: process = arrMonitor[3]
        if len(arrMonitor) > 4: time_used = arrMonitor[4]
        if len(arrMonitor) > 5: t0 = arrMonitor[5]
        if len(arrMonitor) > 6: t1 = arrMonitor[6]
        """
        group = "OMX"
        source = "OMX_NOTIFICATION_LOG"
        target = "OMX_ORDR_STAT_NTFCTN"
        process = "WF_OMX_PARSER"
        time_used = "9 Minute"
        t0 = "23/02/2021 11:21:54 PM"
        t1 = "23/02/2021 11:31:54 PM"
        """
        I1 = ImageDraw.Draw(img)

        I1.text((column20, row15), "Notice ", fill=(0, 0, 0))
        I1.text((column85, row30), str(datetime.now()), fill=(0, 0, 0))

        I1.text((column50, row60), "Group ", fill=(0, 0, 0))
        I1.text((column100, row60), group, fill=(128, 128, 128))

        I1.text((column50, row80), "Source ", fill=(0, 0, 0))
        I1.text((column100, row80), source, fill=(128, 128, 128))

        I1.text((column50, row100), "Target ", fill=(0, 0, 0))
        I1.text((column100, row100), target, fill=(128, 128, 128))

        if (process != None):
            I1.text((column40, row140), str.format("{0} ({1})", process, time_used), fill=(128, 128, 128))
        if (t0 != None):
            I1.text((column50, row160), "Start Time ", fill=(0, 0, 0))
            I1.text((column120, row160), str(t0), fill=(128, 128, 128))
        if (t1 != None):
            I1.text((column50, row180), "End Time ", fill=(0, 0, 0))
            I1.text((column120, row180), str(t1), fill=(128, 128, 128))
        img.save(filealert)

        if os.path.exists(filealert):
            if (token != ""):
                formatmessage = "alert time : {0}"
                headers = {'Authorization': 'Bearer ' + token}
                payload = {'message': " "}  # ,'stickerPackageId':1,'stickerId':1
                files = {'imageFile': open(filealert, 'rb')} if img else None
                r = requests.post(URLLine, headers=headers, params=payload, files=files)
                if files:
                    files['imageFile'].close()
    return str(r.status_code)

@app.route('/line/image_single_list/<token>/<ciphertext>', methods=['GET'])
def send_image_single_list(token=None,ciphertext=None):
    countrow = 0
    row15 = 15
    row30 = 30
    row50 = 50
    row65 = 60
    row80 = 80
    row100 = 100
    row120 = 120
    rowSpace = 20

    column20 = 20
    column40 = 40
    column50 = 50
    column85 = 85
    column100 = 100
    column120 = 120



    fontRed = (255, 0, 0)
    fontBlack = (0, 0, 0)
    fontGreen = (0, 255, 0)
    fontGray = (128, 128, 128)
    fontYellow = (255, 255, 0)
    arrItem = []
    arrKeyValue = []
    arrMonitor = []
    notify = "* Warning Delay time 2 Hour"
    key = ""
    value = ""
    listRecord = ""
    process = ""
    time_used = ""
    t0 = ""
    t1 = ""
    msg = ""
    html = requests.get(URLPdpa.format(keyAPI, ciphertext))
    result = dict(json.loads(html.text))
    if result["value_cipher"] != None:
        msg = (result["value_cipher"])
    filebg = 'notify_single.png'
    filealert = 'notify_single_list_alert.png'
    arrMonitor = (msg).split("*")
    if len(arrMonitor) > 0:
        img = Image.open(filebg)
        if len(arrMonitor) > 0: group = arrMonitor[0]
        if len(arrMonitor) > 1: source = arrMonitor[1]
        if len(arrMonitor) > 2: listRecord = arrMonitor[2]
        I1 = ImageDraw.Draw(img)

        I1.text((column20, row15), "Notice ", fill=(0, 0, 0))
        I1.text((column85, row30), str(datetime.now()), fill=(0, 0, 0))

        I1.text((column50, row50), "Group ", fill=(0, 0, 0))
        I1.text((column100, row50), group, fill=(128, 128, 128))

        I1.text((column50, row65), "Source ", fill=(0, 0, 0))
        I1.text((column100, row65), source, fill=(128, 128, 128))

        arrItem = (listRecord).split("-")
        if len(arrItem) > 0:
            for item in arrItem:
                countrow += 1
                print(item)
                arrKeyValue = item.split(",")
                if len(arrKeyValue)>0:
                    I1.text((column50, row80), "Key", fill=(0, 0, 0))
                    I1.text((column120, row80), "Value", fill=(0, 0, 0))

                if len(arrKeyValue)>0:key=arrKeyValue[0]
                if len(arrKeyValue)>1:value=arrKeyValue[1]

                I1.text((column50, row100), key, fill=(128, 128, 128))
                I1.text((column120, row100), value, fill=(128, 128, 128))
                row100 += rowSpace
        img.save(filealert)

        if os.path.exists(filealert):
            if (token != ""):
                formatmessage = "alert time : {0}"
                headers = {'Authorization': 'Bearer ' + token}
                payload = {'message': " "}  # ,'stickerPackageId':1,'stickerId':1
                files = {'imageFile': open(filealert, 'rb')} if img else None
                r = requests.post(URLLine, headers=headers, params=payload, files=files)
                if files:
                    files['imageFile'].close()
    return str(r.status_code)


# host='0.0.0.0','172.19.234.74'
app.run(debug=True,port=5000)