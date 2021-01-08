split -n l/3 indiatodayurlList.txt x
cp xaa indiatodayurlList.txt
cp xab indiatodayurlListi.txt
cp xac indiatodayurlListii.txt
python3 /mnt/data/Classifierv22.py &> /mnt/data/ClassificationLogs2.txt &
python3 /mnt/data/Classifierv22i.py &> /mnt/data/ClassificationLogs21.txt &
python3 /mnt/data/Classifierv22ii.py &> /mnt/data/ClassificationLogs22.txt &
wait
sed -i 's/:_/###_/g' urlsegmentDatabase22.txt
grep "/share/video/India" urlsegmentDatabase22.txt | sed 's/_religion.and.spirituality_hinduism_@@religion.and.spirituality/_news_national.news_@@news/g' >> urlsegmentDatabase22.txt
grep "/share/video/World" urlsegmentDatabase22.txt | sed 's/_art.and.entertainment_music_music.genres_world.music_@@art.and.entertainment/_news_international.news_@@news/g' >> urlsegmentDatabase22.txt
sed -i 's/www.indiatoday.in\/livetv###_sports_sports.news_@@news/www.indiatoday.in\/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing/g' urlsegmentDatabase22.txt
sed -i 's/www.indiatoday.in\/###_religion.and.spirituality_hinduism_@@religion.and.spirituality/www.indiatoday.in\/###_news__@@news/g' urlsegmentDatabase22.txt
sed -i 's/www.aajtak.in\/india###_art.and.entertainment_movies.and.tv_bollywood_@@art.and.entertainment/www.aajtak.in\/india###_news_national.news_@@news/g' urlsegmentDatabase22.txt
sed -i 's/:_/###_/g' urlsegmentDatabase22i.txt
grep "/share/video/India" urlsegmentDatabase22i.txt | sed 's/_religion.and.spirituality_hinduism_@@religion.and.spirituality/_news_national.news_@@news/g' >> urlsegmentDatabase22i.txt
grep "/share/video/World" urlsegmentDatabase22i.txt | sed 's/_art.and.entertainment_music_music.genres_world.music_@@art.and.entertainment/_news_international.news_@@news/g' >> urlsegmentDatabase22i.txt
sed -i 's/www.indiatoday.in\/livetv###_sports_sports.news_@@news/www.indiatoday.in\/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing/g' urlsegmentDatabase22i.txt
sed -i 's/www.indiatoday.in\/###_religion.and.spirituality_hinduism_@@religion.and.spirituality/www.indiatoday.in\/###_news__@@news/g' urlsegmentDatabase22i.txt
sed -i 's/www.aajtak.in\/india###_art.and.entertainment_movies.and.tv_bollywood_@@art.and.entertainment/www.aajtak.in\/india###_news_national.news_@@news/g' urlsegmentDatabase22i.txt
sed -i 's/:_/###_/g' urlsegmentDatabase22ii.txt
grep "/share/video/India" urlsegmentDatabase22ii.txt | sed 's/_religion.and.spirituality_hinduism_@@religion.and.spirituality/_news_national.news_@@news/g' >> urlsegmentDatabase22ii.txt
grep "/share/video/World" urlsegmentDatabase22ii.txt | sed 's/_art.and.entertainment_music_music.genres_world.music_@@art.and.entertainment/_news_international.news_@@news/g' >> urlsegmentDatabase22ii.txt
sed -i 's/www.indiatoday.in\/livetv###_sports_sports.news_@@news/www.indiatoday.in\/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing/g' urlsegmentDatabase22ii.txt
sed -i 's/www.indiatoday.in\/###_religion.and.spirituality_hinduism_@@religion.and.spirituality/www.indiatoday.in\/###_news__@@news/g' urlsegmentDatabase22ii.txt
sed -i 's/www.aajtak.in\/india###_art.and.entertainment_movies.and.tv_bollywood_@@art.and.entertainment/www.aajtak.in\/india###_news_national.news_@@news/g' urlsegmentDatabase22ii.txt
python3 /mnt/data/UpdateScript22.py > /mnt/data/UpdateLogs22.txt
python3 /mnt/data/UpdateScript22i.py > /mnt/data/UpdateLogs22i.txt
python3 /mnt/data/UpdateScript22ii.py > /mnt/data/UpdateLogs22ii.txt
mv /mnt/data/indiatodayurlList.txt /mnt/data/indiatodayurllistbk.txt
mv /mnt/data/indiatodayurlListi.txt /mnt/data/indiatodayurlListibk.txt
mv /mnt/data/indiatodayurlListii.txt  /mnt/data/indiatodayurlListiibk.txt
mv /mnt/data/urlsegmentDatabase22.txt /mnt/data/urlsegmentDatabase22bk.txt
mv /mnt/data/urlsegmentDatabase22i.txt /mnt/data/urlsegmentDatabase22ibk.txt
mv /mnt/data/urlsegmentDatabase22ii.txt /mnt/data/urlsegmentDatabase22iibk.txt

