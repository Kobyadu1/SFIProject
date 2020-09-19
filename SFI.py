import os
import pandas as pd

df = pd.read_csv("USvideos.csv")
df.drop(labels=["video_id","thumbnail_link"],inplace=True,axis=1)


viewScale = .35
likeDisScale = .65
currentDate = " "
newDate = " "
for date in df["trending_date"]:
    if currentDate != date:
        dateArray = date.split('.')
        year = "20"+dateArray[0]
        month = dateArray[2]
        day = dateArray[1]
        newDate = year+"-"+month+"-"+day
        df["trending_date"].replace(date,newDate,inplace=True)
    currentDate = newDate

df["trending_date"] = pd.to_datetime(df["trending_date"])

for x in df['trending_date'].index:
    if df['comment_count'][x] == 0 or df['likes'][x] == 0 or df['dislikes'][x] == 0:
        df.drop(x, axis=0, inplace=True)

g = df.groupby(pd.Grouper(key='trending_date', freq='M'))
dfs = [group for _,group in g]
dfs_processed = []
for data in dfs:
    data['like_to_dislike'] = data['likes'] / data['dislikes']
    data['user_interaction'] = data['views'] / data['comment_count']
    data['amount_of_videos'] = len(data['trending_date'].index)

    data['views'] = data['views'] / data['views'].max()
    data['like_to_dislike'] = data['like_to_dislike'] / data['like_to_dislike'].max()
    data['user_interaction'] = data['user_interaction'] / data['user_interaction'].max()

    data['raw_score'] = data["user_interaction"]/((data['views']*viewScale) + (data['like_to_dislike']*likeDisScale))
    data['raw-score_norm'] = data['raw_score'] / data['raw_score'].max()

    data.drop('comments_disabled', axis=1, inplace=True)
    data.drop('ratings_disabled', axis=1, inplace=True)
    data.drop('video_error_or_removed', axis=1, inplace=True)

    years = str(data['trending_date'].iloc[1]).split("-")[0]
    months = str(data['trending_date'].iloc[1]).split("-")[1]
    data.sort_values(by=['raw-score_norm'],inplace=True)
    dfs_processed.append(data)
    #data.to_csv("C:\\Users\\" + os.getlogin() + "\\Desktop\\dfs\\" + str(years)+str(months) + ".csv", index=False)

for data in dfs_processed:
    top_25 = int((data['amount_of_videos'].iloc[1]-1) * .25)
    bottom_25 = int((data['amount_of_videos'].iloc[1]-1) * .75)
    middle = bottom_25-top_25
    tag_count_top = {}
    tag_count_middle = {}
    tag_count_bottom = {}

    for x in range(1,top_25):
        tags = data['tags'].iloc[x]
        for tag in tags.split("|"):
            if tag in tag_count_top.keys():
                tag_count_top[tag] = tag_count_top[tag] + 1
            else:
                tag_count_top[tag] = 1
    for x in range(top_25, middle):
        tags = data['tags'].iloc[x]
        for tag in tags.split("|"):
            if tag in tag_count_middle.keys():
                tag_count_middle[tag] = tag_count_middle[tag] + 1
            else:
                tag_count_middle[tag] = 1
    for x in range(middle, bottom_25):
        tags = data['tags'].iloc[x]
        for tag in tags.split("|"):
            if tag in tag_count_bottom.keys():
                tag_count_bottom[tag] = tag_count_bottom[tag] + 1
            else:
                tag_count_bottom[tag] = 1

    parsedcount = pd.Series(list(tag_count_top.values()), name='count')
    parsedtag = pd.Series(list(tag_count_top.keys()), name='tag')
    top25 = {'count': parsedcount, 'tag': parsedtag}

    parsedcount = pd.Series(list(tag_count_middle.values()), name='count')
    parsedtag = pd.Series(list(tag_count_middle.keys()), name='tag')
    middle50 = {'count': parsedcount, 'tag': parsedtag}

    parsedcount = pd.Series(list(tag_count_bottom.values()), name='count')
    parsedtag = pd.Series(list(tag_count_bottom.keys()), name='tag')
    bottom25 = {'count': parsedcount, 'tag': parsedtag}

    top25df = pd.DataFrame(top25)
    middle50df = pd.DataFrame(middle50)
    bottom25df = pd.DataFrame(bottom25)

    top25df.sort_values(by=['count'],inplace=True,ascending=False)
    middle50df.sort_values(by=['count'],inplace=True,ascending=False)
    bottom25df.sort_values(by=['count'],inplace=True,ascending=False)

    years = str(data['trending_date'].iloc[1]).split("-")[0]
    months = str(data['trending_date'].iloc[1]).split("-")[1]
    top25df.to_csv("C:\\Users\\" + os.getlogin() + "\\Desktop\\dfs\\" + str(years)+str(months) + "top25.csv", index=False)
    middle50df.to_csv("C:\\Users\\" + os.getlogin() + "\\Desktop\\dfs\\" + str(years) + str(months) + "middle50.csv", index=False)
    bottom25df.to_csv("C:\\Users\\" + os.getlogin() + "\\Desktop\\dfs\\" + str(years) + str(months) + "buttom25.csv", index=False)


