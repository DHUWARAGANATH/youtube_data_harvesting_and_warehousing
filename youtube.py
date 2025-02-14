import googleapiclient.discovery
import pymongo
import pymysql
import pandas as pd
from datetime import datetime
import streamlit as st
from googleapiclient.errors import HttpError

#Api connection
api_service_name = "youtube"
api_version = "v3"
api_key=""
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey= api_key)

#channel_id input
#channel_Id=input()

#channel information
def get_channel_detail(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    for i in response.get('items',[]):
        snippet=i.get('snippet',{})
        contentDetails=i.get('contentDetails',{})
        statistics=i.get('statistics',{})
        data={
            "channel_name":snippet.get('title',''),
            "Channel_ID":i.get('id',''),
            "Subscription_Count":statistics.get('subscriberCount',''),
            "Channel_Views":statistics.get('viewCount',''),
            "Channel_Description":snippet.get('description',''),
            "Video_count":statistics.get('videoCount',''),
            "Playlist_id":contentDetails.get('relatedPlaylists',{}).get('uploads','')}
    return data
       


#video_ids
def get_video_id(channel_id):
    video_ids = []
    request = youtube.channels().list(part="contentDetails",id=channel_id)
    response = request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token= None
    while True:
            response1=youtube.playlistItems().list(part='snippet',playlistId=playlist_id,maxResults=50,pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
            if next_page_token is None:
                break
    return video_ids




#video information
def get_video_detail(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response.get('items', []):
            snippet = item.get('snippet', {})
            statistics = item.get('statistics', {})
            content_details = item.get('contentDetails', {})
            data1 = {
                'Channel_Id': snippet.get('channelId', ''),
                'Channel_Name': snippet.get('channelTitle', ''),
                'Video_Id': item.get('id', ''),
                'Video_Name': snippet.get('title', ''),
                'Video_Description': snippet.get('description', ''),
                'Published_Date': snippet.get('publishedAt', ''),
                'View_Count': statistics.get('viewCount', ''),
                'Like_Count': statistics.get('likeCount', ''),
                'Favorite_Count': statistics.get('favoriteCount', 0),
                'Comment': statistics.get('commentCount', 0),
                'Duration': content_details.get('duration', ''),
                'Thumbnail': snippet.get('thumbnails', {}).get('default', {}).get('url', ''),
                'Caption_status': content_details.get('caption', '')
            }
            video_data.append(data1)
    return video_data
        
#comment information
def get_comment_detail(video_ids):
    comment_data=[]
    for video_id in video_ids:
        try:
            request=youtube.commentThreads().list(
            part="snippet,replies",videoId=video_id,
            maxResults=50)
            response3=request.execute()
            for item in response3.get('items',[]):
                snippet=item.get('snippet',{})
                replies=item.get('replies',{})
                data2= {"Channel_Id":snippet.get("channelId",''),
                        "comment_id":item.get('id',''),
                        "Video_id":snippet.get('videoId',''),
                        "Comment_Text":snippet.get('topLevelComment',{}).get('snippet',{}).get('textDisplay',''),
                        "Comment_Author":snippet.get('topLevelComment',{}).get('snippet',{}).get('authorDisplayName',''),
                        "Comment_PublishedAt":snippet.get('topLevelComment',{}).get('snippet',{}).get('publishedAt','')}
                comment_data.append(data2)
        except HttpError as e:
            if e.resp.status == 403:
                print(f"Comments are disabled for video ID: {video_id}")
                continue
            else:
                print(f"Error fetching comments for video ID: {video_id}, {e}")
    return comment_data





#mongodb connection
client=pymongo.MongoClient('mongodb://localhost:27017')
database=client['youtube_data_harvesting']
collection=database['channel_details']

def all_channel_details(channel_id):
    ch_details=get_channel_detail(channel_id)
    vid_id=get_video_id(channel_id)
    vid_details=get_video_detail(vid_id)
    com_details=get_comment_detail(vid_id)

    database=client['youtube_data_harvesting']
    collection=database['channel_details']
    collection.insert_one({"channel_information":ch_details,
                           "video_information":vid_details,
                           "comment_information":com_details})
    return "Channel informations uploaded successfully"

#mysql connection
myconnection=pymysql.connect(host="127.0.0.1",user="root",password="Datascience@2871")
cur=myconnection.cursor()
cur.execute('CREATE DATABASE IF NOT EXISTS youtube_harvesting')

#creating channel table
def channel_table(channel_iD):
    myconnection=pymysql.connect(host="127.0.0.1",user="root",password="Datascience@2871",database="youtube_harvesting")
    cur=myconnection.cursor()
    cur.execute('CREATE DATABASE IF NOT EXISTS youtube_harvesting')
    sql_channel=cur.execute("""CREATE TABLE IF NOT EXISTS channel_details(channel_name varchar(50),
                Channel_Id VARCHAR(100) PRIMARY KEY,\
                Subscription_Count INT,\
                Channel_Views BIGINT,\
                Channel_Description TEXT,\
                Video_count INT,\
                Playlist_id VARCHAR(100))""")
    myconnection.commit()
    chan_dbs = []
    database = client['youtube_data_harvesting']
    collection = database['channel_details']
    for ch_data in collection.find({},{"_id": 0,"channel_information":1}):
        chan_dbs.append(ch_data['channel_information'])

    ID=channel_iD
    new_chan_dbs=[]
    for item in chan_dbs:
        if item.get('Channel_ID') == ID:
            new_chan_dbs.append(item)
    chan_df=pd.DataFrame(new_chan_dbs)

    
    for index,row in chan_df.iterrows():   
        add_query=("""insert into channel_details(channel_name,
        Channel_Id,
        Subscription_Count,
        Channel_Views,
        Channel_Description,
        Video_count,
        Playlist_id)
        values(%s,%s,%s,%s,%s,%s,%s)""")

        values=(row['channel_name'],
                row['Channel_ID'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Video_count'],
                row['Playlist_id'])
        cur.execute(add_query,values)
        myconnection.commit()

#creating video table
def video_table(channel_iD):
    myconnection=pymysql.connect(host="127.0.0.1",user="root",password="Datascience@2871",database="youtube_harvesting")
    cur=myconnection.cursor()
    cur.execute('CREATE DATABASE IF NOT EXISTS youtube_harvesting')
    sql_video=cur.execute("""CREATE TABLE IF NOT EXISTS video_details(Channel_Id varchar(100),Channel_Name varchar(100),Video_Id varchar(100) PRIMARY KEY,
                Video_Name VARCHAR(500) ,\
                Video_Description TEXT,\
                Published_Date DATETIME,\
                View_Count INT,\
                Like_Count INT,\
                Favorite_Count INT,\
                Comment INT,\
                Duration TIME,\
                Thumbnail VARCHAR(100),
                Caption_status varchar(50))""")
    myconnection.commit()
    
    video_dbs = []
    database = client['youtube_data_harvesting']
    collection = database['channel_details']
    for video_data in collection.find({},{"_id": 0,"video_information":1}):
        for i in range(len(video_data['video_information'])):
            video_dbs.append(video_data['video_information'][i])
    
    ID=channel_iD
    new_vid_dbs=[]
    for item in video_dbs:
        if item.get('Channel_Id') == ID:
            new_vid_dbs.append(item)
    video_df=pd.DataFrame(new_vid_dbs)
    
    video_df['Published_Date']=pd.to_datetime(video_df['Published_Date'])
    video_df['Duration'] = pd.to_timedelta(video_df['Duration']).dt.total_seconds()
    video_df['Duration'] = pd.to_datetime(video_df['Duration'], unit='s').dt.strftime('%H:%M:%S')
    video_df = video_df.where(pd.notnull(video_df), None)
    
    

    for index,row in video_df.iterrows():
        add_query=("""insert into video_details(Channel_Id,
        Channel_Name,
        Video_Id,
        Video_Name,
        Video_Description,
        Published_Date,
        View_Count,
        Like_Count,
        Favorite_Count,
        Comment,
        Duration,
        Thumbnail,
        Caption_status)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        values=(row['Channel_Id'],
                row['Channel_Name'],
                row['Video_Id'],
                row['Video_Name'],
                row['Video_Description'],
                row['Published_Date'],
                row['View_Count'],
                row['Like_Count'],
                row['Favorite_Count'],
                row['Comment'],
                row['Duration'],
                row['Thumbnail'],
                row['Caption_status'])
        cur.execute(add_query,values)
        myconnection.commit()


#creating comment table
def comment_table(channel_iD):
    myconnection=pymysql.connect(host="127.0.0.1",user="root",password="Datascience@2871",database="youtube_harvesting")
    cur=myconnection.cursor()
    cur.execute('CREATE DATABASE IF NOT EXISTS youtube_harvesting')
    sql_comment=cur.execute("""CREATE TABLE IF NOT EXISTS comment_details(Channel_Id varchar(100),comment_id varchar(100) PRIMARY KEY,Video_id varchar(50),
                Comment_Text TEXT,\
                Comment_Author TEXT,\
                Comment_PublishedAt DATETIME)""")
    myconnection.commit()
    comment_dbs = []
    database = client['youtube_data_harvesting']
    collection = database['channel_details']
    for comment_data in collection.find({},{"_id": 0,"comment_information":1}):
        for i in range(len(comment_data['comment_information'])):
            comment_dbs.append(comment_data['comment_information'][i])
    ID=channel_iD      
    new_com_dbs=[]
    for item in comment_dbs:
        if item.get('Channel_Id') == ID:
            new_com_dbs.append(item)
    comment_df=pd.DataFrame(new_com_dbs)      
    comment_df=pd.DataFrame(comment_dbs)
    comment_df['Comment_PublishedAt']=pd.to_datetime(comment_df['Comment_PublishedAt'])
    
    for index,row in comment_df.iterrows():
            try:
                add_query=("""insert into comment_details(Channel_Id,
                comment_id,
                Video_id,
                Comment_Text,
                Comment_Author,
                Comment_PublishedAt)
                values(%s,%s,%s,%s,%s,%s)""")

                values=(row['Channel_Id'],
                        row['comment_id'],
                        row['Video_id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_PublishedAt'])
                cur.execute(add_query,values)
                myconnection.commit()
            except pymysql.IntegrityError as e:
                print("Skipping duplicate entry:", e)

def all_tables(channel_iD):
    channel_table(channel_iD)
    video_table(channel_iD)
    comment_table(channel_iD)

    return "All tables are created"
#result=all_tables()


#streamlit
st.image("logo.png")
st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
tab1, tab2, tab3 = st.tabs(["Data Collection", "Migrate Data TO Sql","Data Analysis"])

def view_channel_details():
    chan_dbs = []
    database = client['youtube_data_harvesting']
    collection = database['channel_details']
    for ch_data in collection.find({},{"_id": 0,"channel_information":1}):
        chan_dbs.append(ch_data['channel_information'])

    df=st.dataframe(chan_dbs)
    return df

def view_video_details():
    video_dbs = []
    database = client['youtube_data_harvesting']
    collection = database['channel_details']
    for video_data in collection.find({},{"_id": 0,"video_information":1}):
        for i in range(len(video_data['video_information'])):
            video_dbs.append(video_data['video_information'][i])
    df2=st.dataframe(video_dbs)
    return df2

def view_comment_details():
    comment_dbs = []
    database = client['youtube_data_harvesting']
    collection = database['channel_details']
    for comment_data in collection.find({},{"_id": 0,"comment_information":1}):
        for i in range(len(comment_data['comment_information'])):
            comment_dbs.append(comment_data['comment_information'][i])

    df3=st.dataframe(comment_dbs)
    return df3

with tab1:
    #st.markdown(style)
    st.subheader("Enter the YouTube Channel ID")    
    channel_Id=st.text_input('Enter a 24-character ID:')
    result = st.success("Input accepted: {}".format(channel_Id)) if len(channel_Id) == 24 else (st.error("Input should be exactly 24 characters long.") or None)
    if st.button("Collect and store data"):
        chan_ids = []
        database = client['youtube_data_harvesting']
        collection = database['channel_details']
        for ch_data in collection.find({},{"_id": 0,"channel_information":1}):
            chan_ids.append(ch_data['channel_information']['Channel_ID'])
        if channel_Id in chan_ids:
            st.success('The channel_id already exist')
        else:
            insert=all_channel_details(channel_Id)
            st.success(insert)

    View=st.checkbox('View Collections')
    if View:     
        show_table=st.selectbox("SELECT THE COLLECTION FOR VIEW", ("CHANNELS", "VIDEOS","COMMENTS"))
        if show_table=="CHANNELS":
            view_channel_details()
        elif show_table=="VIDEOS":
            view_video_details()
        elif show_table=="COMMENTS":
            view_comment_details()

with tab2:
    st.subheader("Transfering Data to Sql")
    channel_ID=st.text_input('Enter a Channel ID Stored  in Database:',key="channel_id_input")
    result2 = st.success("Input accepted: {}".format(channel_ID)) if len(channel_ID) == 24 else (st.error("Input should be exactly 24 characters long.") or None)
    
    if st.button("Migrate to sql"):
        tables=all_tables(channel_ID)
        st.success(tables)

   
with tab3:
    st.subheader("Sql Query Output Display")
    view2=st.checkbox("View Query")
    if view2:
        myconnection=pymysql.connect(host="127.0.0.1",user="root",password="Datascience@2871",database="youtube_harvesting")
        cur=myconnection.cursor()


        question=st.selectbox("Select your question",("What are the names of all the videos and their corresponding channels?",
                                                    "Which channels have the most number of videos, and how many videos do they have?",
                                                    "What are the top 10 most viewed videos and their respective channels?",
                                                    "How many comments were made on each video, and what are their corresponding video names?",
                                                    "Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "What are the names of all the channels that have published videos in the year 2022?",
                                                    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "Which videos have the highest number of comments, and what are their corresponding channel names?"))

        if question=="What are the names of all the videos and their corresponding channels?":
            query1="select Video_Name,Channel_Name from video_details"
            cur.execute(query1)
            myconnection.commit()
            q1=cur.fetchall()
            df1=pd.DataFrame(q1,columns=["video_name","channel_name"])
            st.write(df1)
            
        elif question=="Which channels have the most number of videos, and how many videos do they have?":  
            query2="select channel_name,Video_count from channel_details order by Video_count desc"
            cur.execute(query2)
            myconnection.commit()
            q2=cur.fetchall()
            df2=pd.DataFrame(q2,columns=["channel_name","video_count"])
            st.write(df2)

        elif question=="What are the top 10 most viewed videos and their respective channels?":
            query3="select Channel_Name,Video_Name,View_Count from video_details order by view_count desc limit 10"
            cur.execute(query3)
            myconnection.commit()
            q3=cur.fetchall()
            df3=pd.DataFrame(q3,columns=["channel_name","video_name","view_count"])
            st.write(df3)
            
        elif question=="How many comments were made on each video, and what are their corresponding video names?":
            query4="select Video_Name,Comment from video_details order by Comment desc"
            cur.execute(query4)
            myconnection.commit()
            q4=cur.fetchall()
            df4=pd.DataFrame(q4,columns=["video_name","number_of_comments"])
            st.write(df4)
            
        elif question=="Which videos have the highest number of likes, and what are their corresponding channel names?":
            query5="select Channel_Name,Video_Name,Like_Count from video_details order by Like_Count desc"
            cur.execute(query5)
            myconnection.commit()
            q5=cur.fetchall()
            df5=pd.DataFrame(q5,columns=["channel_name","video_name","like_count"])
            st.write(df5)
            
        elif question=="What is the total number of views for each channel, and what are their corresponding channel names?":
            query6="select channel_name,Channel_Views from channel_details order by Channel_Views desc"
            cur.execute(query6)
            myconnection.commit()
            q6=cur.fetchall()
            df6=pd.DataFrame(q6,columns=["channel_name","channel_views"])
            st.write(df6)

        elif question=="What are the names of all the channels that have published videos in the year 2022?":
            query7="select Channel_Name,Video_Name,Published_Date from video_details where extract(year from Published_Date)=2022"
            cur.execute(query7)
            myconnection.commit()
            q7=cur.fetchall()
            df7=pd.DataFrame(q7,columns=["channel_name","video_name","published_year"])
            st.write(df7)

        elif question=="What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query8="SELECT Channel_Name, AVG(Duration) AS Avg_duration FROM video_details GROUP BY Channel_Name"
            cur.execute(query8)
            myconnection.commit()
            q8=cur.fetchall()
            df8=pd.DataFrame(q8,columns=["channel_name","Avg_duration"])
            st.write(df8)

        elif question=="Which videos have the highest number of comments, and what are their corresponding channel names?":
            query9="select Channel_Name,Video_Name,Comment from video_details order by Comment desc"
            cur.execute(query9)
            myconnection.commit()
            q9=cur.fetchall()
            df10=pd.DataFrame(q9,columns=["channel_name","video_name","highest_no_of_comments"])
            st.write(df10)   
