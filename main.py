# libraries used in this project
import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import mysql.connector

# Set up YouTube Data API client
youtube =build("youtube", "v3", developerKey="AIzaSyAKlK_fY0xaA9KjLKxy7ErWBav9De5MbkY")

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
mydb = client["youtube_data"]
mycol = mydb["get_data"]

# Connect to MySQL
mydb_sql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database="youtube"
)
cursor = mydb_sql.cursor()

def table_exists(table_name):
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return cursor.fetchone() is not None

# Create table if it doesn't exist
if not table_exists("channel_details"):
    cursor.execute("CREATE TABLE channel_details(channel_id varchar(70) NOT NULL PRIMARY KEY, channelName varchar(70), subcription int(70), views int(100), total_videos int(20), playlist_id varchar(70), description text(400000), publishedAt varchar(70), viewCount int(70))")
if not table_exists("video_details"):
    cursor.execute("create table video_details(video_id varchar(70) NOT NULL PRIMARY KEY, channelTitle varchar(70), title varchar(500), description text(400000), publishedAt varchar(70), viewCount int(70), likeCount int(90), favouriteCount varchar(56), commentCount int(90), duration varchar(70), definition varchar(70), caption varchar(90))")
if not table_exists("playlist_details"):
    cursor.execute("create table playlist_details(playlist_id varchar(70) NOT NULL PRIMARY KEY, channelId varchar(70), playlist_title varchar(89), Playlist_video_count int(90))")
if not table_exists("comment_details"):
    cursor.execute("create table comment_details(comment_id varchar(250) NOT NULL PRIMARY KEY, video_id varchar(70), textDisplay text(1000), authorDisplayName varchar(70), publishedAt varchar(70))")

# creating functions for retrieving each data from youtube API:
# creating function for channel details:
def get_channel_details(channel_id):
    channel_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
    )
    response = request.execute()
#loop through items
    for item in response["items"]:
        data={'channel_id':item["id"],
        'channelName':item["snippet"]["title"],
        'subcription':item["statistics"]["subscriberCount"],
        'views':item["statistics"]["viewCount"],
        'total_videos':item["statistics"]["videoCount"],
        'playlist_id':item["contentDetails"]["relatedPlaylists"]["uploads"],
        'description':item["snippet"]["description"],
        'publishedAt':item["snippet"]["publishedAt"],
        'viewCount':item["statistics"]["viewCount"],
        }
        channel_data.append(data)
    return channel_data

# creating function playlist
def each_playlist(channel_id):
    playlist_data=[]
    request = youtube.playlists().list(
    part="id, contentDetails, snippet",
    channelId=channel_id,
    maxResults = 10
    )
    response = request.execute()
    for i in response['items']:
        list={'playlist_id' :i['id'],
              'channelId' :i['snippet']['channelId'],
              'playlist_title' :i['snippet']['title'],
              'Playlist_video_count' :i['contentDetails']['itemCount']}
        playlist_data.append(list)

    return playlist_data

# creating function for video id:
def get_video_ids(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
    return video_ids

# creating function for video details:
def get_video_details(youtube, video_ids):
    videodetails_data = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50]))
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                             }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            videodetails_data.append(video_info)

    return videodetails_data

# creating function for comments:
def final_comments(youtube, video_ids):
    comments_data = []
    for i in range(len(video_ids)):
        request = youtube.commentThreads().list(
            part="snippet, replies",
            videoId=video_ids[i],
            maxResults=50

        )
        try:
            response = request.execute()
            if response['items']:
                for i in response['items']:
                    comments_data.append({'comment_id': i['snippet']['topLevelComment']['id'],
                                         'video_id': i['snippet']['topLevelComment']['snippet']['videoId'],
                                         'textDisplay': i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                         'authorDisplayName': i['snippet']['topLevelComment']['snippet'][
                                             'authorDisplayName'],
                                         'publishedAt': i['snippet']['topLevelComment']['snippet']['publishedAt']
                                         })

            else:
                pass
        except:
            pass

    return comments_data

# Display channel and video details
def display_channel_data(channel_data, playlist_data, video_data, comments_data):
    st.subheader("Channel Details")
    st.write(channel_data, index =[0])

    st.subheader("playlist items")
    st.write(playlist_data)

    st.subheader("Video Details")
    st.write(video_data)

    st.subheader("Comments Details")
    st.write(comments_data)
# Store data in MongoDB
def store_mongodb(channel_data, playlist_data, video_data, comments_data):

    data = {
        "_id":channel_data['channelName'],
        "channel_data": channel_data,
        "playlist_data": playlist_data,
        "video_stats": video_data,
        "comments_data": comments_data}
    mycol.insert_one(data)

# store data in MySQL:
def store_in_sql(selected_document):
    # store documents in mysql table
    channel_data = pd.DataFrame(selected_document['channel_data'], index = [0])
    for i in channel_data.index:
        row = channel_data.iloc[i]
        query = 'insert into channel_details values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        values = tuple(row.to_dict().values())
        try:
            cursor.execute(query, values)
        except:
            pass
    # store playlist data
    playlist_data = pd.DataFrame(selected_document['playlist_data'])
    for i in playlist_data.index:
        row = playlist_data.iloc[i]
        query = 'insert into playlist_details values(%s,%s,%s,%s)'
        values = list(row.to_dict().values())
        try:
            cursor.execute(query, values)
        except:
            pass

    # store video data
    video_stats = pd.DataFrame(selected_document['video_stats'])
    for i in video_stats.index:
        row = video_stats.iloc[i]
        query = 'insert video_details values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        values = tuple(row.to_dict().values())
        try:
            cursor.execute(query, values)
        except:
            pass

    # store comments data
    comments_data = pd.DataFrame(selected_document['comments_data'])
    for i in comments_data.index:
        row = comments_data.iloc[i]
        query = 'insert comment_details values(%s, %s, %s, %s, %s)'
        values = tuple(row.to_dict().values())
        try:
            cursor.execute(query, values)

        except:
            pass
    st.success("Document uploaded to MySQL!")
    mydb_sql.commit()

# creating function for data scraping :
def get_data():
    channel_ids = st.sidebar.text_input("Enter YouTube Channel ID")
    if st.sidebar.button("Get Channel Details"):
        for channel_id in channel_ids.strip().split(','):
            channel_data = get_channel_details(channel_id)[0]
            playlist_id = channel_data['playlist_id']
            playlist_data = each_playlist(channel_id)[0:]
            video_ids = get_video_ids(youtube, playlist_id)[0:]
            video_data = get_video_details(youtube, video_ids)[0:]
            comments_data= final_comments(youtube, video_ids)[0:]
            st.write(display_channel_data(channel_data, playlist_data, video_data, comments_data))
            store_mongodb(channel_data, playlist_data, video_data, comments_data)
            st.success(" Data Scraped ")

# Get document IDs from MongoDB collection
document_ids = [str(doc["_id"]) for doc in mycol.find()]

# creating function to migrate data to MySQL:
def migrate():
    st.title("Migrate data to MySQL")
    # Display dropdowns to select MongoDB document and MySQL table
    selected_document_id = st.sidebar.selectbox("Select MongoDB Document", document_ids)
    # Retrieve selected document from MongoDB
    selected_document = mycol.find_one({"_id": selected_document_id})
    if st.sidebar.button("Upload Document to MYSQL"):
        store_in_sql(selected_document)
        st.subheader("Channel Details")
        st.write(pd.DataFrame(selected_document['channel_data'], index = [0]))

        st.subheader("playlist Details")
        st.write(pd.DataFrame(selected_document['playlist_data']))

        st.subheader("Video Details")
        st.write(pd.DataFrame(selected_document['video_stats']))

        st.subheader("Comments Details")
        st.write(pd.DataFrame(selected_document['comments_data']))


def query():
    st.subheader("Join Operations")
    getquerydata = st.sidebar.selectbox('select option', ["What are the names of all the videos and their corresponding channels?",
                                                          "Which channels have the most number of videos, and how many videos do they have?",
                                                          "What are the top 10 most viewed videos and their respective channels?",
                                                          "How many comments were made on each video, and what are their corresponding video names?",
                                                          "Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                          "What is the total number of likes and comments for each video, and what are their corresponding video names?",
                                                          "What is the total number of views for each channel, and what are their corresponding channel names?",
                                                          "What are the names of all the channels that have published videos in the year 2022?",
                                                          "What are the top 10 videos got likes and their respective channels?",
                                                          "Which videos have the highest number of comments, and what are their corresponding channel names?"])

    if getquerydata == "What are the names of all the videos and their corresponding channels?":
        st.subheader("#query1 -  What are the names of all the videos and their corresponding channels?")
        cursor.execute( "select channel_details.channelName, video_details.title from channel_details join video_details ON channel_details.channelName=video_details.channelTitle")
        result1 = cursor.fetchall()
        st.write(pd.DataFrame(result1, columns=['Video Title', 'Channel Name']))

    elif getquerydata == "Which channels have the most number of videos, and how many videos do they have?":
        st.subheader("query2 - Which channels have the most number of videos, and how many videos do they have?")
        query2 = "SELECT cd.channelName, cd.total_videos FROM channel_details AS cd ORDER BY total_videos DESC"
        cursor.execute(query2)
        result2 = cursor.fetchall()
        st.write(pd.DataFrame(result2, columns=['Channel Name', 'Video Count']))

    elif getquerydata == "What are the top 10 most viewed videos and their respective channels?":
        st.subheader("query3 - What are the top 10 most viewed videos and their respective channels?")
        query3 = "SELECT vd.channelTitle,vd.viewCount from video_details AS vd ORDER BY vd.viewCount DESC LIMIT 10"
        cursor.execute(query3)
        result3 = cursor.fetchall()
        st.write(pd.DataFrame(result3, columns=['Channel_name', 'views']))

    elif getquerydata == "How many comments were made on each video, and what are their corresponding video names?":
        st.subheader("query4 - How many comments were made on each video, and what are their corresponding video names?")
        query4 = "SELECT vd.channelTitle,vd.title,vd.commentCount from video_details AS vd"
        cursor.execute(query4)
        result4 = cursor.fetchall()
        st.write(pd.DataFrame(result4, columns=['channel_name', 'video_title', 'comments']))

    elif getquerydata == "Which videos have the highest number of likes, and what are their corresponding channel names?":
        st.subheader("query5 - Which videos have the highest number of likes, and what are their corresponding channel names?")
        query5 = "SELECT vd.channelTitle,vd.title,vd.likeCount from video_details AS vd ORDER BY vd.likeCount DESC LIMIT 1"
        cursor.execute(query5)
        result5 = cursor.fetchall()
        st.write(pd.DataFrame(result5, columns=['channel_name', 'video_title', 'likes']))

    elif getquerydata == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        st.subheader("query6 - What is the total number of likes and comments for each video, and what are their corresponding video names?")
        query6 = "SELECT vd.channelTitle,vd.title,vd.likeCount,vd.commentCount from video_details AS vd"
        cursor.execute(query6)
        result6 = cursor.fetchall()
        st.write(pd.DataFrame(result6, columns=['channel_name', 'video_title', 'likes', 'comments']))

    elif getquerydata == "What is the total number of views for each channel, and what are their corresponding channel names?":
        st.subheader("query7 - What is the total number of views for each channel, and what are their corresponding channel names?")
        query7 = "SELECT cd.channelName,cd.views from channel_details AS cd ORDER BY views DESC"
        cursor.execute(query7)
        result7 = cursor.fetchall()
        st.write(pd.DataFrame(result7, columns=['channel_name', 'views']))

    elif getquerydata == "What are the names of all the channels that have published videos in the year 2022?":
        st.subheader("query8 - What are the names of all the channels that have published videos in the year 2022?")
        year = 2022
        query8 = "SELECT vd.channelTitle, vd.title, vd.publishedAt FROM video_details AS vd WHERE YEAR(vd.publishedAt) = %s"
        cursor.execute(query8, (year,))
        result8 = cursor.fetchall()
        st.write(pd.DataFrame(result8, columns=['channel_name', 'video_title', 'published_date']))

    elif getquerydata == "What are the top 10 videos got likes and their respective channels?":
        st.subheader("query9 - What are the top 10 videos got likes and their respective channels?")
        query9 = "SELECT vd.channelTitle,vd.title,vd.likeCount from video_details AS vd ORDER BY vd.likeCount DESC "
        cursor.execute(query9)
        result9 = cursor.fetchall()
        st.write(pd.DataFrame(result9, columns=['channel_name', 'video_title', 'likes']))

    elif getquerydata == "Which videos have the highest number of comments, and what are their corresponding channel names?":
        st.subheader("query10 - Which videos have the highest number of comments, and what are their corresponding channel names?")
        query10 = "SELECT vd.channelTitle,vd.title,vd.commentCount from video_details AS vd ORDER BY vd.commentCount DESC LIMIT 1"
        cursor.execute(query10)
        result10 = cursor.fetchall()
        st.write(pd.DataFrame(result10, columns=['channel_name', 'video_title', 'comments_count']))

getyoutubedata = st.sidebar.selectbox('select option', ['getdata', 'migrate', 'myquery'])
if getyoutubedata =='getdata':
    st.title('Youtube Data Harvesting')
    get_data()
elif getyoutubedata=='migrate':
    migrate()
elif getyoutubedata=='myquery':
    query()


