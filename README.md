BackupImageCacher
=================

Write a script which creates AWS Glacier Archives containing daily and monthly
backups of the image_cacher.pl output images. The same script should includes a
cleanup mode in which it deletes all of the daily Glacier Archives that are now
included in a monthly Glacier Archive.
