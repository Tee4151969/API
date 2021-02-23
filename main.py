
from flask import Flask, json
from fuzzywuzzy import fuzz
import pandas as pd
import time
from langdetect import detect
import pythainlp.util as pyUtil

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
app.run(debug=True, host='0.0.0.0', port=80)