from bs4 import BeautifulSoup
from pyspark.sql import HiveContext
import requests, json, os, sys, time, re

def parse_app_detail(detail):
    if not detail:
        return {}
    
    about_the_game = detail.get('about_the_game')
    if about_the_game:
        about_the_game = BeautifulSoup(about_the_game).text.replace('\r', '').replace('\n', '')
    
    appid = detail.get('steam_appid')
    
    background = detail.get('background')
    
    categories = []
    for c in detail.get('categories', {}):
        categories.append(c.get('description'))
    categories = ','.join(set(categories))
        
    detailed_description = detail.get('detailed_description')
    if detailed_description:
        detailed_description = BeautifulSoup(detailed_description).text.replace('\r', '').replace('\n', '')
        
    developers = detail.get('developers')
    if developers:
        developers = ','.join(set(developers))
    
    fullgame_appid = detail.get('fullgame', {}).get('appid')
    
    fullgame_name = detail.get('fullgame', {}).get('name')

    genres = []
    for g in detail.get('genres', {}):
        genres.append(g.get('description'))
    genres = ','.join(set(genres))
    
    header_image = detail.get('header_image')
    
    is_free = detail.get('is_free')
    
    name = detail.get('name')
    
    platforms_windows = detail.get('platforms', {}).get('windows')
    
    platforms_mac = detail.get('platforms', {}).get('mac')
    
    platforms_linux = detail.get('platforms', {}).get('linux')
    
    price_overview = detail.get('price_overview', {}).get('initial')
    if price_overview:
        price_overview /= 100
    else:
        price_overview = 0.0
    
    publishers = detail.get('publishers')
    if publishers:
        publishers = ','.join(publishers)
    else:
        publishers = None
    
    if not detail.get('release_date', {}).get('coming_soon'):
        release_date = detail.get('release_date', {}).get('date')
        if release_date:
            try:
                release_date = datetime.strptime(release_date, '%b %d, %Y').date()
            except Exception as e:
                try:
                    release_date = datetime.strptime(release_date, '%d %b, %Y').date()
                except:
                    try:
                        release_date = datetime.strptime(release_date, '%b %Y').date()
                    except:
                        release_date = None
    else:
        release_date = None
    
    screenshots = []
    for s in detail.get('screenshots', {}):
        screenshots.append(s.get('path_full'))
    screenshots = ','.join(set(screenshots))
        
    short_description = detail.get('short_description')
    
    support_info_url = detail.get('support_info', {}).get('url')
    
    support_info_email = detail.get('support_info', {}).get('email')
    
    supported_languages = detail.get('supported_languages')
    if supported_languages:
        supported_languages = BeautifulSoup(supported_languages).text.replace('*', '').replace('-', ',')\
                                .replace('\r', ',').replace('\n', ',').replace(';', '').replace('#', '')\
                                .replace('languages with full audio support','').replace('Interface:','')\
                                .replace('Audio:','').replace('Subtitles:','').replace('Full','')\
                                .replace('Flemish','').replace('English Dutch  English', 'English, Dutch')
        temp_list = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", supported_languages).split(',')
        languages_list = []
        for l in temp_list:
            if l.strip():
                languages_list.append(l.strip())
        supported_languages = ','.join(set(languages_list))
    
    apptype = detail.get('type')
    
    website = detail.get('website')
    
    app_detail = {
        'about_the_game': about_the_game,
        'appid': appid,
        'background': background,
        'categories': categories,
        'detailed_description': detailed_description,
        'developers': developers,
        'fullgame_appid': fullgame_appid,
        'fullgame_name': fullgame_name,
        'genres': genres,
        'header_image': header_image,
        'is_free': is_free,
        'name': name,
        'platforms_windows': platforms_windows,
        'platforms_mac': platforms_mac,
        'platforms_linux': platforms_linux,
        'price_overview': price_overview,
        'publishers': publishers,
        #'release_date': release_date,
        'screenshots': screenshots,
        'short_description': short_description,
        'support_info_url': support_info_url,
        'support_info_email': support_info_email,
        'supported_languages': supported_languages,
        'apptype': apptype,
        'website': website
    }
    
    spark.sql("INSERT INTO appinfo ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                                    '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                                    (about_the_game, appid, background, categories, detailed_description, developers,\
                                     fullgame_appid, fullgame_name, genres, header_image, is_free, name,\
                                     platforms_windows, platforms_mac, platforms_linux, price_overview, publishers,\
                                     screenshots, short_description, support_info_url, support_info_email,\
                                     supported_languages, apptype, website))
    
    return app_detail
