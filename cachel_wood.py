import time
import csv
import gevent
import requests
import asyncio
import aiofiles
from io import BytesIO
from PIL import Image
import requests as req
from bs4 import BeautifulSoup
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

urls = []
analysis_url = []
  
def coroutine1():   #协程1执行producer1的所有url任务
    job_list1 = []  # 保存所有协程任务
    for n in range(0,1300,35):
        url = f'https://music.163.com/discover/playlist/?order=hot&cat=%E8%AF%B4%E5%94%B1&limit=35&offset={n}'
        print(url)
        job1 = gevent.spawn(producer1, url)  # 执行一个协程任务
        job_list1.append(job1)  # 把每个协程任务放进一个列表中保存
        gevent.joinall(job_list1)  # 等待所有协程结束

def coroutine2():  #协程2执行consumer1的所有url任务
    row = ['id','title','nickname','img','description','count','number of song','number of adding list','share','comment']
    with open('data.csv','a',encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(row)
    job_list2 = []
    for url in urls:
        job2 = gevent.spawn(consumer1, url)  # 执行一个协程任务
        job_list2.append(job2)  # 把每个协程任务放进一个列表中保存
        gevent.joinall(job_list2)  # 等待所有协程结束

#使用生产者消费者模式，生产者产生的id链接传给消费者执行
def producer1(url):  
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    }
    response = requests.get(url=url,headers=headers,verify=False)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser') 

    ids = soup.select('.dec a')      # 获取包含歌单详情页网址的标签
    for i in ids:
        page_url = 'https://music.163.com/' + i['href']  #生产者传递的id链接
        #print(page_url)
        urls.append(page_url)

def consumer1(url):  #将获取歌单的信息写入csv文件
    with open('data.csv','a',encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }

        response = requests.get(url=url,headers=headers,verify=False)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser') 

        idd = soup.select('.s-fc7')[0]['href'].split('=')[-1]   #获取歌单id
        img = soup.select('img')[0]['data-src']   #图片链接
        res = req.get(img)
        image = Image.open(BytesIO(res.content))  #图片处理
        try:
            image.save(str(time.time())+'.jpg')
        except:
            image.save(str(time.time())+'.png')
            #os.remove(os.getcwd()+f'\\{cnt}.jpg')

        title = soup.select('title')[0].get_text()  #标题
        nickname = soup.select('.s-fc7')[0].get_text()  #昵称
        #print(idd,title,nickname)

        description = soup.select('p')[1].get_text()  #简介
        count = soup.select('strong')[0].get_text()   #播放次数
        song_number = soup.select('span span')[0].get_text()  #歌的数目
        add_lis = soup.select('a i')[1].get_text()   #添加进列表次数
        share = soup.select('a i')[2].get_text()    #分享次数
        comment = soup.select('a i')[4].get_text()  #评论次数
        #print(description,count,song_number,add_lis,share,comment)
        
        csv_writer.writerow([idd,title,nickname,img,description,count,song_number,add_lis,share,comment])
        
        if float(count)>1000000:  #提取播放数超过1百万的歌单
            #yield url
            res = requests.get(url,headers=headers,verify=False)
            soup = BeautifulSoup(res.text,'html.parser')

            song = soup.select('li a')
            for s in song[:10]:
                analysis_url.append('https://music.163.com/'+s['href']) #添加歌曲id进列表

async def write_demo(songname,singer,album):  #异步实现文件的写
    # 异步方式执行with操作,修改为 async with
    async with aiofiles.open("song.txt","a",encoding="utf-8") as file:
        await file.write(songname+','+singer+','+album+'\n')

def consumer2(url):   
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    }
    res = requests.get(url,headers=headers,verify=False)
    soup = BeautifulSoup(res.text,'html.parser')

    songname = soup.select('.f-ff2')[0].string  #获取歌名
    singer = soup.select('p a')[0].string   #获取歌手
    album = soup.select('p a ')[1].string   #获取专辑
    asyncio.run(write_demo(songname,singer,album))  #异步实现写入

def coroutine3():   #协程3执行consumer2的所有url
    with open('song.txt','a') as file:
        file.write('songname,singer,album\n')
    job_list3 = []
    for url in analysis_url:
        job3 = gevent.spawn(consumer2, url)  # 执行一个协程任务
        job_list3.append(job3)  # 把每个协程任务放进一个列表中保存
        gevent.joinall(job_list3)  # 等待所有协程结束

def main():
    start_time = time.time()
    coroutine1()
    coroutine2()
    coroutine3()
    print('time = %f'%(time.time()-start_time))

main()
