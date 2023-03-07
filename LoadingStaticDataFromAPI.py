import requests
import json
from datetime import datetime
import os
import io
import pandas as pd
import certifi
import ast
from decouple import config

# global dropins, facilities, locations, registeredPrograms
googleAPIKey = config('GOOGLEAPIKEY')
googleAPIUrl = 'https://maps.googleapis.com/maps/api/geocode/json?address='
PROVINCE = 'Ontario'


def getResoursesFromAPIs():
    global dropins, facilities, locations, registeredPrograms
    LOCATIONS = 'Locations'
    DROPIN = 'Drop-in.json'
    FACILITIES = 'Facilities.json'
    REGISTERED_PROGRAMS = 'Registered Programs.json'
    apiurl = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=da46e4ac-d4ab-4b1c-b139-6362a0a43b3c'
    params = {'key': 'value'}

    r = requests.get(url=apiurl, params=params)
    response = r.json()

    resoursesJSON = response['result']['resources']
    resourses = {}

    for resourseJSON in resoursesJSON:
        if resourseJSON['name'] in [DROPIN, FACILITIES, REGISTERED_PROGRAMS]:
            name = resourseJSON['name']
            url = resourseJSON['url']
            content = requests.get(url=url, params=params).json()
            resourses[name] = content
        elif resourseJSON['name'] == LOCATIONS:
            url = resourseJSON['url']
            csv = requests.get(url=url, params=params).content
            locations = pd.read_csv(io.StringIO(
                csv.decode('utf-8')), sep=',', header=0)
            locations = locations.fillna('')

    dropins = resourses[DROPIN]
    facilities = resourses[FACILITIES]
    registeredPrograms = resourses[REGISTERED_PROGRAMS]


def getAvalibilities():
    avalibilities = []
    for dropin in dropins:
        avalibility = {}
        avalibility['_id'] = dropin['_id']
        avalibility['Location ID'] = dropin['Location ID']
        avalibility['Course_ID'] = dropin['Course_ID']
        avalibility['Course Title'] = dropin['Course Title']
        avalibility['age_min'] = dropin['Age Min']
        avalibility['age_max'] = dropin['Age Max']
        avalibility['start_time'] = dropin['Start Date Time']
        startDatetime = datetime.strptime(
            dropin['Start Date Time'], '%Y-%m-%dT%H:%M:%S')
        endHour = dropin['End Hour']
        endMin = dropin['End Min']
        endDatetime = startDatetime.replace(hour=endHour, minute=endMin)
        endDatetimeStr = endDatetime.strftime('%Y-%m-%dT%H:%M:%S')
        avalibility['end_time'] = endDatetimeStr
        avalibility['Category'] = dropin['Category']
        avalibilities.append(avalibility)
    return avalibilities


def writeListToTxt(filename, mode, list):
    with open(os.getcwd() + '/' + filename + '.txt', mode) as fp:
        for item in list:
            fp.write("%s\n" % item)


def getActivities():
    activities = set()
    for dropin in dropins:
        activity = dropin['Course Title']
        activities.add(activity)
    return activities


def getType():
    activities = getActivities()
    types = set()
    for activity in activities:
        if ':' in activity:
            type = activity.split(':')[0]
        else:
            type = activity
        types.add(type)
    return types


def getCategory():
    categories = set()
    for dropin in dropins:
        category = dropin['Category']
        categories.add(category)
    return categories


def getFaciltiesNoGeo():
    availablities = getAvalibilities()
    locationList = locations.filter(
        items=['Location ID', 'Location Name', 'District', 'Street No', 'Street No Suffix', 'Street Name', 'Street Type', 'Postal Code']).values.tolist()

    locationIDs = set()
    ficilityAddressLocation = []

    for availablity in availablities:
        locationID = availablity['Location ID']
        locationIDs.add(locationID)

    for locationID in locationIDs:
        for locat in locationList:
            if locationID == locat[0]:
                # print(locat)
                # if locat[4] == 'NaN':
                #     locat[4] = ''
                # elif locat[7] == 'NaN':
                #     locat[7] = ''

                street = str(locat[3]) + str(locat[4]) + ' ' + str(locat[5]) + ' ' + str(locat[6])
                ficilityAddressLocation.append(
                    {'Location ID': locat[0], 'Facility Name': locat[1], 'city': locat[2], 'Street': street, 'province': PROVINCE, 'postal_code': locat[7]})

    return ficilityAddressLocation


def getGeoToFacilities():
    facilities = getFaciltiesNoGeo()
    for facility in facilities:
        # addressStr = 'Baycrest Arena 160 Neptune Dr. North York Ontario'
        addressStr = facility['Street'] + ' ' + facility['city'] + ' ' + facility['province']
        addressStr = addressStr.replace(' ', '%20')
        url = googleAPIUrl + addressStr + '&key=' + googleAPIKey
        # print(url)
        params = {'key': 'value'}
        r = requests.get(url=url, params=params)
        response = r.json()
        # print(response)
        geometry = response['results'][0]['geometry']['location']
        facility['lat'] = geometry['lat']
        facility['lng'] = geometry['lng']
        if facility['postal_code'] == '':
            print(url)
            facility['postal_code'] = response['results'][0]['address_components'][-1]['short_name']
            print(facility['postal_code'])
    return facilities


getResoursesFromAPIs()

writeListToTxt("Facilities", "a", getGeoToFacilities())
