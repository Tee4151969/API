#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# Use multiple tokens
import psutil
"""
tokens = ["Sgw45zUs6lYvJpN6mCBgEXrJA3wAkr7eg4Zo1UEaxFk","JhKbIif7Nac2qmI5cvFq6WxMPFoesWffmZtf0mHxLI6"]
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

def _lineNotify(payload,file=None):
    url = "https://notify-api.line.me/api/notify'
    token = token_num #EDIT
    headers = {"Authorization":"Bearer "+token}
    return requests.post(url, headers=headers , data = payload, files=file)

    disk_partitions = psutil.disk_partitions(all=False)
    diskspace = "(หน่วยนับ GB)\r\n"
    fmt_str = "{:<7} {:<7} {:<7} {:<7} {:<7}"
    print(fmt_str.format("Drive", "Total", "Used", "Free", "Percent(%)"))
    diskspace += fmt_str.format("Drive", "Total", "Used", "Free", "%")
    diskspace += "\r\n"
    for partition in disk_partitions:
    usage = psutil.disk_usage(partition.mountpoint)
    fmt_str = "{:<8} {:<8} {:<8} {:<8} {:<8}"
    print(fmt_str.format(partition.device, usage.total // (2**30), usage.used // (2**30), usage.free // (2**30), usage.percent))
    diskspace += fmt_str.format(partition.device, usage.total // (2**30), usage.used // (2**30), usage.free // (2**30), usage.percent)
    diskspace += "\r\n"

for x in range(0,len(tokens)):
    token_num = tokens[x]
    lineNotify(diskspace)
"""
# notifyFile("./logo.png")
