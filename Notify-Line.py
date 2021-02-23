#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import requests
def lineNotify(message):
    payload = {"message":message}
    return _lineNotify(payload)
def notifyFile(filename):
    file = {"imageFile":open(filename,"rb")}
    payload = {"message": "test"}
    return _lineNotify(payload,file)
def notifyPicture(url):
    payload = {"message":" ","imageThumbnail":url,"imageFullsize":url}
    return _lineNotify(payload)
def notifySticker(stickerID,stickerPackageID):
    payload = {"message":" ","stickerPackageId":stickerPackageID,"stickerId":stickerID}
    return _lineNotify(payload)
def _lineNotify(payload,token,file=None):
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization":"Bearer "+token}
    return requests.post(url, headers=headers , data = payload, files=file)