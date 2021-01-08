split -n l/3 indiatodayurlListv1.txt x
cp xaa indiatodayurlListv1.txt
cp xab indiatodayurlListv1i.txt
cp xac indiatodayurlListv1ii.txt
python3 /mnt/data/Classifierv2.py &> /mnt/data/ClassificationLogs.txt &
python3 /mnt/data/Classifierv2i.py &> /mnt/data/ClassificationLogs1.txt &
python3 /mnt/data/Classifierv2ii.py &> /mnt/data/ClassificationLogs2.txt &
wait
sed -i 's/:_/###_/g' urlsegmentDatabase.txt
grep "/share/video/India" urlsegmentDatabase.txt | sed 's/_religion.and.spirituality_hinduism_@@religion.and.spirituality/_news_national.news_@@news/g' >> urlsegmentDatabase.txt
grep "/share/video/World" urlsegmentDatabase.txt | sed 's/_art.and.entertainment_music_music.genres_world.music_@@art.and.entertainment/_news_international.news_@@news/g' >> urlsegmentDatabase.txt
sed -i 's/www.indiatoday.in\/livetv###_sports_sports.news_@@news/www.indiatoday.in\/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing/g' urlsegmentDatabase.txt
sed -i 's/www.indiatoday.in\/###_religion.and.spirituality_hinduism_@@religion.and.spirituality/www.indiatoday.in\/###_news__@@news/g' urlsegmentDatabase.txt
sed -i 's/www.aajtak.in\/india###_art.and.entertainment_movies.and.tv_bollywood_@@art.and.entertainment/www.aajtak.in\/india###_news_national.news_@@news/g' urlsegmentDatabase.txt
sed -i 's/:_/###_/g' urlsegmentDatabasei.txt
grep "/share/video/India" urlsegmentDatabasei.txt | sed 's/_religion.and.spirituality_hinduism_@@religion.and.spirituality/_news_national.news_@@news/g' >> urlsegmentDatabasei.txt
grep "/share/video/World" urlsegmentDatabasei.txt | sed 's/_art.and.entertainment_music_music.genres_world.music_@@art.and.entertainment/_news_international.news_@@news/g' >> urlsegmentDatabasei.txt
sed -i 's/www.indiatoday.in\/livetv###_sports_sports.news_@@news/www.indiatoday.in\/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing/g' urlsegmentDatabasei.txt
sed -i 's/www.indiatoday.in\/###_religion.and.spirituality_hinduism_@@religion.and.spirituality/www.indiatoday.in\/###_news__@@news/g' urlsegmentDatabasei.txt
sed -i 's/www.aajtak.in\/india###_art.and.entertainment_movies.and.tv_bollywood_@@art.and.entertainment/www.aajtak.in\/india###_news_national.news_@@news/g' urlsegmentDatabasei.txt
sed -i 's/:_/###_/g' urlsegmentDatabaseii.txt
grep "/share/video/India" urlsegmentDatabaseii.txt | sed 's/_religion.and.spirituality_hinduism_@@religion.and.spirituality/_news_national.news_@@news/g' >> urlsegmentDatabaseii.txt
grep "/share/video/World" urlsegmentDatabasei.txt | sed 's/_art.and.entertainment_music_music.genres_world.music_@@art.and.entertainment/_news_international.news_@@news/g' >> urlsegmentDatabaseii.txt
sed -i 's/www.indiatoday.in\/livetv###_sports_sports.news_@@news/www.indiatoday.in\/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing/g' urlsegmentDatabaseii.txt
sed -i 's/www.indiatoday.in\/###_religion.and.spirituality_hinduism_@@religion.and.spirituality/www.indiatoday.in\/###_news__@@news/g' urlsegmentDatabaseii.txt
sed -i 's/www.aajtak.in\/india###_art.and.entertainment_movies.and.tv_bollywood_@@art.and.entertainment/www.aajtak.in\/india###_news_national.news_@@news/g' urlsegmentDatabaseii.txt
echo "https://www.aajtak.in/###_news__@@news" >> urlsegmentDatabase.txt
echo "https://www.aajtak.in/###_news__@@news" >> urlsegmentDatabase.txt
echo "https://www.aajtak.in/indiatoday-livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing" >> urlsegmentDatabase.txt
echo "https://www.aajtak.in/livetv###_technology.and.computing_software_desktop_video_@@technology.and.computing" >> urlsegmentDatabase.txt
python3 /mnt/data/UpdateScript.py > /mnt/data/UpdateLogs.txt
python3 /mnt/data/UpdateScripti.py > /mnt/data/UpdateLogsi.txt
python3 /mnt/data/UpdateScriptii.py > /mnt/data/UpdateLogsii.txt
mv /mnt/data/indiatodayurlListv1.txt /mnt/data/indiatodayurllistbk.txt
mv /mnt/data/indiatodayurlListv1i.txt /mnt/data/indiatodayurlListibk.txt
mv /mnt/data/indiatodayurlListv1ii.txt  /mnt/data/indiatodayurlListiibk.txt
mv /mnt/data/urlsegmentDatabase.txt /mnt/data/urlsegmentDatabasebk.txt
mv /mnt/data/urlsegmentDatabasei.txt /mnt/data/urlsegmentDatabaseibk.txt
mv /mnt/data/urlsegmentDatabaseii.txt /mnt/data/urlsegmentDatabaseiibk.txt

