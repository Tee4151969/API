
from flask import Flask, json
from fuzzywuzzy import fuzz
import pandas as pd
import time
from langdetect import detect
import pythainlp.util as pyUtil
import pyodbc as im
import datetime
import numpy as np
app = Flask(__name__)
df = pd.read_csv("Masters.csv")
@app.route('/')
def index():
    return df.to_html()

@app.route('/log', methods=['GET'])
def get_log():
    dflog = pd.read_csv("Log.csv")
    return dflog.to_html()

@app.route('/postcode', methods=['GET'])
def get_postcode():
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))
    df2 = pd.pivot_table(df[['ProvinceThai','DistrictThaiShort','TambonThaiShort','PostCodeMain','ProvinceID']], index=['ProvinceThai','TambonThaiShort', 'DistrictThaiShort','PostCodeMain'], values='ProvinceID', aggfunc=len)
    df2.columns = ["PostCode"]
    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "postcode,"+start_time+","+end_time+","+str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message+'\n')
    f.close()

    return df2.to_json()

@app.route('/subdistrict', methods=['GET'])
def get_subdistrict():
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))
    df2 = pd.pivot_table(df[['ProvinceThai','DistrictThaiShort','TambonThaiShort','ProvinceID']], index=['ProvinceThai','TambonThaiShort', 'DistrictThaiShort'], values='ProvinceID', aggfunc=len)
    df2.columns = ["subdistrict"]
    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "postcode,"+start_time+","+end_time+","+str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message+'\n')
    f.close()

    return df2.to_json()

@app.route('/subdistrict_filter/<param>', methods=['GET'])
def get_subdistrict_filter(param:None):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    arrParam = param.split("-")
    parampv = "";
    paramdst = "";

    if (len(arrParam) > 0):
        parampv = str(arrParam[0])
    if (len(arrParam) > 1):
        paramdst = str(arrParam[1])
    dfsubdistrict = df[(df['ProvinceThai'] == parampv) & (df['DistrictThaiShort'] == paramdst)]
    df2 = pd.pivot_table(dfsubdistrict[['TambonThaiShort','ProvinceID']], index=['TambonThaiShort'], values='ProvinceID', aggfunc=len)
    df2.columns = ["Tambon"]
    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Sub District," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return df2.to_json()

@app.route('/district', methods=['GET'])
def get_district():
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    df2 = pd.pivot_table(df[['ProvinceThai','DistrictThaiShort','ProvinceID']], index=['ProvinceThai', 'DistrictThaiShort'], values='ProvinceID', aggfunc=len)
    df2.columns = ["District"]
    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "District," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return df2.to_json()

@app.route('/district_filter/<param>', methods=['GET'])
def get_district_filter(param=None):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    dfdistrict = df[(df['ProvinceThai'] == param)]
    df2 = pd.pivot_table(dfdistrict[['DistrictThaiShort','ProvinceID']], index=['DistrictThaiShort'], values='ProvinceID', aggfunc=len)
    df2.columns = ["District"]
    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "District," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return df2.to_json()
@app.route('/province', methods=['GET'])
def get_province():
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    df2 = pd.pivot_table(df[['ProvinceThai', 'ProvinceID']],index=['ProvinceThai'], values='ProvinceID', aggfunc=len)
    df2.columns = ["Province"]
    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Province," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return df2.to_json()
    #return json.dumps(companies)
    #TambonID, TambonThai, TambonEng, TambonThaiShort, TambonEngShort, PostCodeMain, PostCodeAll, DistrictID, DistrictThai, DistrictEng, DistrictThaiShort, DistrictEngShort, ProvinceID, ProvinceThai, ProvinceEng




@app.route('/province_fuzzy_score/<param>', methods=['GET'])
def get_fuzzy_province_score(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))
    result = []
    dfResultscore = pd.DataFrame(columns=['fuzzy', 'type', 'score' ])
    df['Provincejoin'] = df['ProvinceThai'] + "/" + df['ProvinceEng']
    arrProvince = df['Provincejoin'].unique().tolist();


    for item in arrProvince:
        correctThai = "";
        correctEng = "";
        arrPv = item.split("/")
        correctThai = arrPv[0];
        correctEng = arrPv[1];

        isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%
            new_rowscore = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
            dfResultscore = dfResultscore.append(new_rowscore, ignore_index=True)
            new_rowscore = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
            dfResultscore = dfResultscore.append(new_rowscore, ignore_index=True)
            new_rowscore = {'fuzzy': correctThai, 'type': 'token', 'score': token}
            dfResultscore = dfResultscore.append(new_rowscore, ignore_index=True)
        else:
            ratio = (fuzz.ratio(correctEng, param))  # 91%
            partial = (fuzz.partial_ratio(correctEng, param))  # 91%
            token = (fuzz.token_set_ratio(correctEng, param))  # 91%
            new_rowscore = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
            dfResultscore = dfResultscore.append(new_rowscore, ignore_index=True)
            new_rowscore = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
            dfResultscore = dfResultscore.append(new_rowscore, ignore_index=True)
            new_rowscore = {'fuzzy': correctThai, 'type': 'token', 'score': token}
            dfResultscore = dfResultscore.append(new_rowscore, ignore_index=True)

    dfSortscore = dfResultscore.sort_values(by=['score'],ascending=[0])
    for i in dfSortscore.head(100).index:
        fuzzy_text = str(dfSortscore['fuzzy'][i])
        type_text = str(dfSortscore['type'][i])
        score = str(dfSortscore['score'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['type'] = type_text
        data['score'] = score
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Province Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return json.dumps(result)


@app.route('/province_fuzzy/<param>', methods=['GET'])
def get_fuzzy_province(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))
    result = []

    dfResult = pd.DataFrame(columns=['fuzzy', 'ratio', 'partial','token'])
    df['Provincejoin'] = df['ProvinceThai'] + "/" + df['ProvinceEng']
    arrProvince = df['Provincejoin'].unique().tolist();


    for item in arrProvince:
        correctThai = "";
        correctEng = "";
        arrPv = item.split("/")
        correctThai = arrPv[0];
        correctEng = arrPv[1];

        isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%
            new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
            dfResult = dfResult.append(new_row, ignore_index=True)

        else:
            ratio = (fuzz.ratio(correctEng, param))  # 91%
            partial = (fuzz.partial_ratio(correctEng, param))  # 91%
            token = (fuzz.token_set_ratio(correctEng, param))  # 91%
            new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
            dfResult = dfResult.append(new_row, ignore_index=True)


    dfSort = dfResult.sort_values(by=['token', 'partial', 'ratio'], ascending=[0, 0, 0])

    for i in dfSort.head(100).index:
        fuzzy_text = str(dfResult['fuzzy'][i])
        ratio_text = str(dfResult['ratio'][i])
        partial_text = str(dfResult['partial'][i])
        token_text = str(dfResult['token'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['ratio'] = ratio_text
        data['partial'] = partial_text
        data['token'] = token_text
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Province Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()

    return json.dumps(result)


@app.route('/province_district_fuzzy_score/<param>', methods=['GET'])
def get_fuzzy_province_district_score(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))
    isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")
    Textprovince = "";
    Textdistrict = "";

    if not isThai:
        arrCheckThaiall = param.split(",")
        if len(arrCheckThaiall) > 0:
            Textprovince = arrCheckThaiall[0]
        if len(arrCheckThaiall) > 1:
            Textdistrict = arrCheckThaiall[1]
        isThaiProvince = detect(Textprovince) == "th"
        isThaiDistrinct = detect(Textdistrict) == "th"

        print(Textprovince + "=" + str(isThaiProvince))
        print(Textdistrict + "=" + str(isThaiDistrinct))

        isThai = isThaiProvince and isThaiDistrinct


    result = []
    dfResult = pd.DataFrame(columns=['fuzzy', 'type', 'score'])
    df['ProvincejoinDistrict'] =df['ProvinceThai'] + " " +df['DistrictThaiShort']+ "/" + df['ProvinceEng']+ " " +df['DistrictEngShort']+ "/" + df['ProvinceThai']+ " " +df['DistrictEngShort']+ "/" + df['ProvinceEng']+ " " +df['DistrictThaiShort']
    arrDistrict = df['ProvincejoinDistrict'].unique().tolist();
    correctThai = "";
    correctEng = "";


    for item in arrDistrict:
        arrPv = item.split("/")
        if len(arrPv) > 0:
            correctThai = arrPv[0];
        if len(arrPv) > 1:
            correctEng = arrPv[1];
        if len(arrPv) > 2:
            correctPEng = arrPv[2];
        if len(arrPv) > 3:
            correctDEng = arrPv[3];

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%
            new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
            dfResult = dfResult.append(new_row, ignore_index=True)
            new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
            dfResult = dfResult.append(new_row, ignore_index=True)
            new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
            dfResult = dfResult.append(new_row, ignore_index=True)
        else:
            if (isThaiProvince):
                ratio = (fuzz.ratio(correctPEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctPEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctPEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiDistrinct):
                ratio = (fuzz.ratio(correctDEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctDEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctDEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            else:
                ratio = (fuzz.ratio(correctEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)

    dfSort = dfResult.sort_values(by=['score'],ascending=[0]);
    for i in dfSort.head(100).index:
        fuzzy_text = str(dfResult['fuzzy'][i])
        type_text = str(dfResult['type'][i])
        score_text = str(dfResult['score'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['type'] = type_text
        data['score'] = score_text
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "District Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return json.dumps(result)


@app.route('/province_district_fuzzy/<param>', methods=['GET'])
def get_fuzzy_province_district(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))
    isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")
    Textprovince = "";
    Textdistrict = "";

    if not isThai:
        arrCheckThaiall = param.split(",")
        if len(arrCheckThaiall) > 0:
            Textprovince = arrCheckThaiall[0]
        if len(arrCheckThaiall) > 1:
            Textdistrict = arrCheckThaiall[1]
        isThaiProvince = detect(Textprovince) == "th"
        isThaiDistrinct = detect(Textdistrict) == "th"

        print(Textprovince + "=" + str(isThaiProvince))
        print(Textdistrict + "=" + str(isThaiDistrinct))

        isThai = isThaiProvince and isThaiDistrinct


    result = []
    dfResult = pd.DataFrame(columns=['fuzzy', 'ratio', 'partial','token'])
    df['ProvincejoinDistrict'] =df['ProvinceThai'] + " " +df['DistrictThaiShort']+ "/" + df['ProvinceEng']+ " " +df['DistrictEngShort']+ "/" + df['ProvinceThai']+ " " +df['DistrictEngShort']+ "/" + df['ProvinceEng']+ " " +df['DistrictThaiShort']
    arrDistrict = df['ProvincejoinDistrict'].unique().tolist();
    correctThai = "";
    correctEng = "";


    for item in arrDistrict:
        arrPv = item.split("/")
        if len(arrPv) > 0:
            correctThai = arrPv[0];
        if len(arrPv) > 1:
            correctEng = arrPv[1];
        if len(arrPv) > 2:
            correctPEng = arrPv[2];
        if len(arrPv) > 3:
            correctDEng = arrPv[3];

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%
            new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
            dfResult = dfResult.append(new_row, ignore_index=True)
        else:
            if (isThaiProvince):
                ratio = (fuzz.ratio(correctPEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctPEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctPEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiDistrinct):
                ratio = (fuzz.ratio(correctDEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctDEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctDEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            else:
                ratio = (fuzz.ratio(correctEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)

    dfSort = dfResult.sort_values(by=['ratio','partial','token'],ascending=[0,0,0]);
    for i in dfSort.head(100).index:
        fuzzy_text = str(dfResult['fuzzy'][i])
        ratio_text = str(dfResult['ratio'][i])
        partial_text = str(dfResult['partial'][i])
        token_text = str(dfResult['token'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['ratio'] = ratio_text
        data['partial'] = partial_text
        data['token'] = token_text
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "District Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return json.dumps(result)


@app.route('/province_subdistrict_fuzzy/<param>', methods=['GET'])
def get_fuzzy_province_subdistrict(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")
    Textprovince = "";
    Textdistrict = "";
    Textsubdistrict = "";
    print(param)
    if not isThai:
        arrCheckThaiall = param.split(",")
        if len(arrCheckThaiall) > 0:
            Textprovince = arrCheckThaiall[0]
        if len(arrCheckThaiall) > 1:
            Textdistrict = arrCheckThaiall[1]
        if len(arrCheckThaiall) > 2:
            Textsubdistrict = arrCheckThaiall[2]

        isThaiProvince = detect(Textprovince) == "th"
        isThaiDistrict = detect(Textdistrict) == "th"
        isThaiSubDistrict = detect(Textsubdistrict) == "th"


    result = []
    dfResult = pd.DataFrame(columns=['fuzzy', 'ratio', 'partial','token'])
    df['ProvincejoinDistrictjoinSub'] =df['ProvinceThai'] + " " +df['DistrictThaiShort']+ " " +df['TambonThaiShort']+ "/" + df['ProvinceEng'] + " " +df['DistrictEngShort']+ " " +df['TambonEngShort']+ "/" +df['ProvinceThai'] + " " + df['DistrictEngShort'] + " " + df['TambonEngShort']+"/" + df['ProvinceThai'] + " " + df['DistrictThaiShort'] + " " + df['TambonEngShort']+"/" + df['DistrictThaiShort'] + " " + df['ProvinceEng'] + " " + df['TambonEngShort']+"/" + df['DistrictThaiShort'] + " " + df['ProvinceThai'] + " " + df['TambonEngShort']+ "/" + df['TambonThaiShort'] + " " + df['DistrictEngShort'] + " " + df['ProvinceEng']+"/" + df['TambonThaiShort'] + " " + df['DistrictThaiShort'] + " " + df['ProvinceEng']
    arrDistrict = df['ProvincejoinDistrictjoinSub'].unique().tolist();
    for item in arrDistrict:
        arrPv = item.split("/")
        correctThai = arrPv[0];
        correctEng = arrPv[1];
        correct1 = arrPv[2];
        correct2 = arrPv[3];
        correct3 = arrPv[4];
        correct4 = arrPv[5];
        correct5 = arrPv[6];
        correct6 = arrPv[7];

        isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%
            new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
            dfResult = dfResult.append(new_row, ignore_index=True)
        else:
            if (isThaiProvince and not  isThaiDistrict and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct1, param))  # 91%
                partial = (fuzz.partial_ratio(correct1, param))  # 91%
                token = (fuzz.token_set_ratio(correct1, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiProvince and isThaiDistrict and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct2, param))  # 91%
                partial = (fuzz.partial_ratio(correct2, param))  # 91%
                token = (fuzz.token_set_ratio(correct2, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiDistrict and not isThaiProvince and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct3, param))  # 91%
                partial = (fuzz.partial_ratio(correct3, param))  # 91%
                token = (fuzz.token_set_ratio(correct3, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiDistrict and  isThaiProvince and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct4, param))  # 91%
                partial = (fuzz.partial_ratio(correct4, param))  # 91%
                token = (fuzz.token_set_ratio(correct4, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiSubDistrict and not isThaiDistrict and not isThaiProvince):
                ratio = (fuzz.ratio(correct5, param))  # 91%
                partial = (fuzz.partial_ratio(correct5, param))  # 91%
                token = (fuzz.token_set_ratio(correct5, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiSubDistrict and  isThaiDistrict and not isThaiProvince):
                ratio = (fuzz.ratio(correct6, param))  # 91%
                partial = (fuzz.partial_ratio(correct6, param))  # 91%
                token = (fuzz.token_set_ratio(correct6, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            else:
                ratio = (fuzz.ratio(correctEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctEng, param))  # 91%
                new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
                dfResult = dfResult.append(new_row, ignore_index=True)

    dfSort = dfResult.sort_values(by=['ratio','partial','token'],ascending=[0,0,0])
    for i in dfSort.head(100).index:
        fuzzy_text = str(dfResult['fuzzy'][i])
        ratio_text = str(dfResult['ratio'][i])
        partial_text = str(dfResult['partial'][i])
        token_text = str(dfResult['token'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['ratio'] = ratio_text
        data['partial'] = partial_text
        data['token'] = token_text
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Sub District Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return json.dumps(result)


@app.route('/province_subdistrict_fuzzy_score/<param>', methods=['GET'])
def get_fuzzy_province_subdistrict_score(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")
    Textprovince = "";
    Textdistrict = "";
    Textsubdistrict = "";
    print(param)
    if not isThai:
        arrCheckThaiall = param.split(",")
        if len(arrCheckThaiall) > 0:
            Textprovince = arrCheckThaiall[0]
        if len(arrCheckThaiall) > 1:
            Textdistrict = arrCheckThaiall[1]
        if len(arrCheckThaiall) > 2:
            Textsubdistrict = arrCheckThaiall[2]

        isThaiProvince = detect(Textprovince) == "th"
        isThaiDistrict = detect(Textdistrict) == "th"
        isThaiSubDistrict = detect(Textsubdistrict) == "th"


    result = []
    dfResult = pd.DataFrame(columns=['fuzzy', 'type', 'score'])
    df['ProvincejoinDistrictjoinSub'] =df['ProvinceThai'] + " " +df['DistrictThaiShort']+ " " +df['TambonThaiShort']+ "/" + df['ProvinceEng'] + " " +df['DistrictEngShort']+ " " +df['TambonEngShort']+ "/" +df['ProvinceThai'] + " " + df['DistrictEngShort'] + " " + df['TambonEngShort']+"/" + df['ProvinceThai'] + " " + df['DistrictThaiShort'] + " " + df['TambonEngShort']+"/" + df['DistrictThaiShort'] + " " + df['ProvinceEng'] + " " + df['TambonEngShort']+"/" + df['DistrictThaiShort'] + " " + df['ProvinceThai'] + " " + df['TambonEngShort']+ "/" + df['TambonThaiShort'] + " " + df['DistrictEngShort'] + " " + df['ProvinceEng']+"/" + df['TambonThaiShort'] + " " + df['DistrictThaiShort'] + " " + df['ProvinceEng']
    arrDistrict = df['ProvincejoinDistrictjoinSub'].unique().tolist();
    for item in arrDistrict:
        arrPv = item.split("/")
        correctThai = arrPv[0];
        correctEng = arrPv[1];
        correct1 = arrPv[2];
        correct2 = arrPv[3];
        correct3 = arrPv[4];
        correct4 = arrPv[5];
        correct5 = arrPv[6];
        correct6 = arrPv[7];

        isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%

            new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
            dfResult = dfResult.append(new_row, ignore_index=True)
            new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
            dfResult = dfResult.append(new_row, ignore_index=True)
            new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
            dfResult = dfResult.append(new_row, ignore_index=True)
        else:
            if (isThaiProvince and not  isThaiDistrict and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct1, param))  # 91%
                partial = (fuzz.partial_ratio(correct1, param))  # 91%
                token = (fuzz.token_set_ratio(correct1, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiProvince and isThaiDistrict and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct2, param))  # 91%
                partial = (fuzz.partial_ratio(correct2, param))  # 91%
                token = (fuzz.token_set_ratio(correct2, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiDistrict and not isThaiProvince and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct3, param))  # 91%
                partial = (fuzz.partial_ratio(correct3, param))  # 91%
                token = (fuzz.token_set_ratio(correct3, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiDistrict and  isThaiProvince and not isThaiSubDistrict):
                ratio = (fuzz.ratio(correct4, param))  # 91%
                partial = (fuzz.partial_ratio(correct4, param))  # 91%
                token = (fuzz.token_set_ratio(correct4, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiSubDistrict and not isThaiDistrict and not isThaiProvince):
                ratio = (fuzz.ratio(correct5, param))  # 91%
                partial = (fuzz.partial_ratio(correct5, param))  # 91%
                token = (fuzz.token_set_ratio(correct5, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            elif (isThaiSubDistrict and  isThaiDistrict and not isThaiProvince):
                ratio = (fuzz.ratio(correct6, param))  # 91%
                partial = (fuzz.partial_ratio(correct6, param))  # 91%
                token = (fuzz.token_set_ratio(correct6, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)
            else:
                ratio = (fuzz.ratio(correctEng, param))  # 91%
                partial = (fuzz.partial_ratio(correctEng, param))  # 91%
                token = (fuzz.token_set_ratio(correctEng, param))  # 91%

                new_row = {'fuzzy': correctThai, 'type': 'ratio', 'score': ratio}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'partial', 'score': partial}
                dfResult = dfResult.append(new_row, ignore_index=True)
                new_row = {'fuzzy': correctThai, 'type': 'token', 'score': token}
                dfResult = dfResult.append(new_row, ignore_index=True)

    dfSort = dfResult.sort_values(by=['score'],ascending=[0])
    for i in dfSort.head(100).index:
        fuzzy_text = str(dfResult['fuzzy'][i])
        type_text = str(dfResult['type'][i])
        score_text = str(dfResult['score'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['type'] = type_text
        data['score'] = score_text
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Sub District Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return json.dumps(result)

@app.route('/province_postcode_fuzzy/<param>', methods=['GET'])
def get_fuzzy_province_postcode(param):
    start_time_second = time.time()
    start_time = (time.strftime("%b %d %Y %H:%M:%S"))

    result = []
    dfResult = pd.DataFrame(columns=['fuzzy', 'ratio', 'partial','token'])
    df['Provincejoinpostcode'] =df['ProvinceThai'] + " " +df['DistrictThaiShort']+ " " +df['TambonThaiShort']+ " " +df['PostCodeAll']+ "/" + df['ProvinceEng'] + " " +df['DistrictEngShort']+ " " +df['TambonEngShort']+ " " +df['PostCodeAll']
    arrDistrict = df['Provincejoinpostcode'].unique().tolist();
    for item in arrDistrict:
        arrPv = item.split("/")
        correctThai = arrPv[0];
        correctEng = arrPv[1];

        isThai = pyUtil.isthai(param, ignore_chars="1234567890#/.-,$ฯ() ")

        if isThai:
            ratio = (fuzz.ratio(correctThai, param))  # 91%
            partial = (fuzz.partial_ratio(correctThai, param))  # 91%
            token = (fuzz.token_set_ratio(correctThai, param))  # 91%
            new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
            dfResult = dfResult.append(new_row, ignore_index=True)
        else:
            ratio = (fuzz.ratio(correctEng, param))  # 91%
            partial = (fuzz.partial_ratio(correctEng, param))  # 91%
            token = (fuzz.token_set_ratio(correctEng, param))  # 91%
            new_row = {'fuzzy': correctThai, 'ratio': ratio, 'partial': partial, 'token': token}
            dfResult = dfResult.append(new_row, ignore_index=True)


    dfSort = dfResult.sort_values(by=['ratio','partial','token'],ascending=[0,0,0])
    for i in dfSort.head(100).index:
        fuzzy_text = str(dfResult['fuzzy'][i])
        ratio_text = str(dfResult['ratio'][i])
        partial_text = str(dfResult['partial'][i])
        token_text = str(dfResult['token'][i])
        data = {}
        data['fuzzy'] = fuzzy_text
        data['ratio'] = ratio_text
        data['partial'] = partial_text
        data['token'] = token_text
        result.append(data)

    end_time_second = time.time()
    end_time = (time.strftime("%b %d %Y %H:%M:%S"))
    message = "Postcode Fuzzy," + start_time + "," + end_time + "," + str(end_time_second - start_time_second)
    f = open("log.csv", "a")
    f.write(message + '\n')
    f.close()
    return json.dumps(result)

@app.route('/fuzzy/<param>', methods=['GET'])
def get_fuzzy(param):
    print(param)
    arrType = param.split(",");
    if len(arrType) == 1:
        return get_fuzzy_province(param)
    elif len(arrType) == 2:
        return get_fuzzy_province_district(param)
    elif len(arrType) == 3:
        return get_fuzzy_province_subdistrict(param)
    else:
        return get_fuzzy_province_postcode(param)

@app.route('/fuzzy_score/<param>', methods=['GET'])
def get_fuzzy_score(param):
    print(param)
    arrType = param.split(",");
    if len(arrType) == 1:
        return get_fuzzy_province_score(param)
    elif len(arrType) == 2:
        return get_fuzzy_province_district_score(param)
    elif len(arrType) == 3:
        return get_fuzzy_province_subdistrict_score(param)
    else:
        return get_fuzzy_province_postcode(param)

@app.route('/fuzzy_compare/<param>', methods=['GET'])
def get_fuzzy_compare(param):
    ratio = 0
    partial = 0
    token = 0
    source_compare = ''
    target_compare = ''
    data = {'fuzzy-source': source_compare, 'fuzzy-target': target_compare, 'ratio': ratio, 'partial': partial,
            'token': token}
    arrCompare = param.split("-")
    print(len(arrCompare))
    if len(arrCompare) == 2:
        source_compare=arrCompare[0]
        target_compare=arrCompare[1]

        print(source_compare)
        print(target_compare)
        ratio = fuzz.ratio(source_compare, target_compare)
        partial = fuzz.partial_ratio(source_compare, target_compare)
        token = fuzz.token_set_ratio(source_compare, target_compare)
        data = {'fuzzy-source': source_compare,'fuzzy-target': target_compare, 'ratio': ratio , 'partial': partial ,'token' : token}

    return json.dumps(data)


@app.route('/fuzzy_ai/<thaiid>', methods=['GET'])
def set_fuzzy_by_thai(thaiid):

    partial = 0
    ratio = 0
    token = 0
    iloop = 0
    irow = 0

    i70 = 0
    i80 = 0
    i90 = 0
    ilow  = 0
    arrAddrKeySortAsc = []
    arrAddrKeySortDesc = []

    sql = " select  t.match_key from adhcisrep.tee_ww_prod_key_before t"
    sql += " where t.idnt_nbr = '{idnt_nbr}'"

    key1 = ""
    key2 = ""
    key3 = ""
    urlformat = "http://winthehouse.dwh-journey.arctic.true.th/fuzzy_compare/{compare}"
    findcompare = ""

    sTableQueryInsert = " INSERT into adhcisrep.tee_ww_prod_key_manual(idnt_nbr,source_addr_key,target_addr_key,partial_score,ratio_score,token_score,ppn_tm) VALUES "
    sIns = ""
    try:
        connection = im.connect('DSN=Impala', autocommit=True)
        print(connection)
        t0 = time.time()
        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= start process  query ============= ')

        dsExeCursor = connection.cursor()

        dsCursor = connection.cursor()
        dsCursor.execute(sql.format(idnt_nbr=thaiid))
        print(sql.format(idnt_nbr=thaiid))
        result = dsCursor.fetchall()
        print(len(result))
        if len(result) > 0:
            sIns = ""
            arrAddrKeySortAsc = []
            arrAddrKeySortDesc = []
            for row in result:
                match_key = row[0]

                arrAddrKeySortAsc.append(match_key)
                arrAddrKeySortDesc.append(match_key)

        arrAddrKeySortAsc = list(dict.fromkeys(arrAddrKeySortAsc))
        arrAddrKeySortDesc = list(dict.fromkeys(arrAddrKeySortDesc))
        arrAddrKeySortAsc.sort()
        arrAddrKeySortDesc.sort(reverse=True)
        if (arrAddrKeySortAsc != None and arrAddrKeySortDesc != None):
            arrAddrCompare = []
            for itemasc in arrAddrKeySortAsc:
                key1 = itemasc
                for itemdesc in arrAddrKeySortDesc:
                    key2 = itemdesc
                    if (key1 != key2):
                        findcompare = key1 + "-" + key2
                        url = (urlformat.format(compare=findcompare))
                        print(url)
                        try:
                            response = json.loads(requests.get(url).text)
                        except:
                            response = None
                        if (response):
                            partial = (response['partial'])
                            ratio = (response['ratio'])
                            token = (response['token'])
                            compare = {"souce": key1, "target": key2, "ratio": ratio, "partial": partial,
                                       "token": token}

                        arrAddrCompare.append(compare)
                        iloop += 1
                        if (ratio >69 or partial >69 or token >69):
                            i70 +=1
                        elif (ratio >79 or partial >79 or token >79):
                            i80 += 1
                        elif (ratio >89 or partial >89 or token >89):
                            i90 += 1
                        else:
                            ilow +=1

                        if sIns != "":
                            sIns += ","
                        sIns += " ('" + thaiid + "', '" + key1 + "', '" + key2 + "', " + str(partial) + ", " + str(ratio) + ", " + str(token) + ",now()) "

                        fromatcounter = "Thai ID = {0} Qty , Loop = {1} Qty "
                        print(fromatcounter.format(str(thaiid), str(iloop)))

                        if (sIns != ""):
                            dsExeCursor.execute(sTableQueryInsert + sIns)
        #return json.dumps({'status': 'OK' ,'record' : str(iloop),'0-69' : str(ilow) ,'70-79' : str(i70) ,'80-89' : str(i80) ,'90-99' : str(i90)})
        return json.dumps({'status': 'OK' ,'record' : str(iloop),'table' : 'adhcisrep.tee_ww_prod_key_manual'})
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        djobtime = datetime.today()
        jobtime = djobtime.strftime('%d/%m/%Y %H:%M:%S')
        if connection: connection.close()


@app.route('/hadoop/listtable/<database>', methods=['GET'])
def get_hadoop_listtable(database):
    result = []
    djobtime = datetime.today()
    try:
        message = ""
        messagerow = ""
        connection = im.connect('DSN=Impala', autocommit=True)
        print(connection)
        t0 = time.time()

        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process source query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")

        print(nowts + '============= start process target  query ============= ')
        sqltable = "show tables in " + database
        print('============= sql command ============= ' + '\n' + sqltable)

        cursorresult = connection.cursor()
        cursorresult.execute(sqltable)
        result = cursorresult.fetchall()

        now = datetime.now()

        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process  target  query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        rerunprovince = ""
        isize = 0
        dsize = 0
        ilen = 0
        for s in result:
            table = s[0]
            try:
                tablefullname = database + "." + table
                sql = "show table stats " + tablefullname
                print(sql)
                cursor = connection.cursor()
                cursor.execute(sql)
                stattable = cursor.fetchone()
                if stattable:
                    strow = str(stattable[0])
                    stfile = str(stattable[1])
                    stsize = stattable[2]
                    stbcache = stattable[3]
                    stscache = stattable[4]
                    stformat = stattable[5]
                    stincre = stattable[6]
                    stlocation = stattable[7]
                    messagerow += tablefullname + "," + stsize + "\n"
                    if (str(stsize).find("GB") > 0):
                        ilen = str(stsize).find("GB")
                        isize = 1073741824
                        dsize = float(str(stsize)[0:ilen]) * isize
                    elif (str(stsize).find("MB") > 0):
                        ilen = str(stsize).find("MB")
                        isize = 1048576
                        dsize = float(str(stsize)[0:ilen]) * isize
                    elif (str(stsize).find("KB") > 0):
                        ilen = str(stsize).find("KB")
                        isize = 1024
                        dsize = float(str(stsize)[0:ilen]) * isize
                    elif (str(stsize).find("B") > 0):
                        ilen = str(stsize).find("B")
                        isize = 1
                        dsize = float(str(stsize)[0:ilen]) * isize

                    messagerow = tablefullname + "," + str(dsize) + "," + stsize
                    data = {}
                    data['table'] = tablefullname
                    data['size'] = str(dsize)
                    data['size(unit)'] = stsize
                    result.append(data)
                    print(messagerow)
                    message += tablefullname + "," + strow + "," + stfile + "," + stsize + "," + stbcache + "," + stscache + "," + stformat + "," + stincre + "," + stlocation
                    message += "\n"

            except Exception as ee:
                messagerow = tablefullname + ",0,not avalible"
                data = {}
                data['table'] = tablefullname
                data['size'] = "0"
                data['size(unit)'] = "not avalible"
                result.append(data)
                print('Error! Code: {c}, Message, {m}'.format(c=type(ee).__name__, m=str(ee)))
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connection.close()
    return json.dumps(result)


@app.route('/hadoop/listcolumn/<table>', methods=['GET'])
def get_hadoop_listcolumn(table):
    try:
        message = ""
        messagerow = ""
        sqlTable = "select {column} from {table} t "
        connection = im.connect('DSN=Impala', autocommit=True)
        t0 = time.time()
        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process source query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        tablespecific = table
        sql = "SHOW column  stats "+tablespecific
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columnname = ""
        columnnametype=""
        tablename = "t."
        iseq = 0
        HTML = ""
        htmlformat = "<tr>"
        htmlformat += "\n<td>{seq}</td>"
        htmlformat += "\n<td>{key}</td>"
        htmlformat += "\n<td>{column}</td>"
        htmlformat += "\n<td>{type}</td>"
        htmlformat += "\n<td>{description}</td>"
        htmlformat += "\n</tr>"

        for c in result:
            iseq += 1
            cname = c[0]
            type = c[1]

            if (columnname!=""):
                columnname +="\n,"
            if (columnnametype!=""):
                columnnametype +="\n,"
            columnname += tablename+(c[0])
            columnnametype += "cast("+ tablename + (c[0]) +" as " + type+") as "+(c[0])
            HTML = htmlformat.format(seq = str(iseq), key = "", column = cname, type = type,description="")
            print(HTML)
        print(columnnametype)
        print(columnname)
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connection.close()

    return sqlTable.format(column=columnname,table=table)

@app.route('/hadoop/listcolumntype/<table>', methods=['GET'])
def get_hadoop_listcolumntype(table):
    try:
        message = ""
        messagerow = ""
        sqlTable = "select {column} from {table} t "
        connection = im.connect('DSN=Impala', autocommit=True)
        t0 = time.time()
        now = datetime.now()
        nowts = now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
        print(nowts + '============= end process source query ============= ')
        print(f"elapsed time {(time.time() - t0):0.1f} seconds")
        tablespecific = table
        sql = "SHOW column  stats "+tablespecific
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columnname = ""
        columnnametype=""
        tablename = "t."
        iseq = 0
        HTML = ""
        htmlformat = "<tr>"
        htmlformat += "\n<td>{seq}</td>"
        htmlformat += "\n<td>{key}</td>"
        htmlformat += "\n<td>{column}</td>"
        htmlformat += "\n<td>{type}</td>"
        htmlformat += "\n<td>{description}</td>"
        htmlformat += "\n</tr>"

        for c in result:
            iseq += 1
            cname = c[0]
            type = c[1]
            if (str(type).lower().find("string")>-1 or str(type).lower().find("varchar")>-1):
                sqlmaxlength = "select max(length({column})) as max_length from {table}".format(column = cname , table = tablespecific)
                cursormax = connection.cursor()
                cursormax.execute(sqlmaxlength)
                resultmax = cursormax.fetchone()

                if resultmax:
                    maxtext = resultmax[0]
                    type = "varchar({ml})".format(ml=maxtext)

            if (columnname!=""):
                columnname +="\n,"
            if (columnnametype!=""):
                columnnametype +="\n,"
            columnname += tablename+(c[0])
            columnnametype += "cast("+ tablename + (c[0]) +" as " + type+") as "+(c[0])
            HTML = htmlformat.format(seq = str(iseq), key = "", column = cname, type = type,description="")
    except Exception as e:
        print('Error! Code: {c}, Message, {m}'.format(c=type(e).__name__, m=str(e)))
    finally:
        connection.close()

    return sqlTable.format(column=columnnametype,table=table)


app.run(debug=True, host='0.0.0.0', port=80)