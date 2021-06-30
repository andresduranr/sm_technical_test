sourcemeriuser DATABASE sm_technicaltest;
 
CREATE user 'sourcemeriuser'@'localhost' identified BY 'holamundo123'

GRANT ALL PRIVILEGES ON sm_technicaltest.* TO 'sourcemeriuser'@'localhost';