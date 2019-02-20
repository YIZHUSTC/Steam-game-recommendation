import requests, json, os, sys, time, re

def get_game_list(user_id):
    base_url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/'
    keys = [key1, key2]
    curr_keyid = 0
    params = {
            'key' : keys[curr_keyid],
            'steamid' : user_id.strip(),
            'format' : 'json' }
    
    games = None
    for i in range(3):
        try:
            games = requests.get(base_url, params = params).json().get('response').get('games')
            break
        except:
            try:
                curr_keyid = 1 - curr_keyid
                params.update({'key' : keys[curr_keyid]})
                games = requests.get(base_url, params = params).json().get('response').get('games')
                break
            except:
                time.sleep(5)
                pass

            
    if games and len(games) > 0:
        gamelist = []
        f = open('userinfo', 'a')
        for g in games:
            gamelist.append(g)
            g.update({'userid' : int(user_id.strip())})
            f.write(json.dumps(g))
            f.write('\n')
            userid = int(user_id.strip())
            appid = g.get('appid')
            playtime_forever = g.get('appid')
            spark.sql('INSERT INTO userinfo (userid, appid, playtime_forever) VALUES (userid, appid, playtime_forever)')
        return gamelist
