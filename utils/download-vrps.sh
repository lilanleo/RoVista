f=$(date +\%Y/\%m/\%d)
d=$(date +\%Y-\%m-\%d)
wget https://ftp.ripe.net/rpki/apnic.tal/$f/roas.csv -O /tmp/apnic-roas.csv
wget https://ftp.ripe.net/rpki/afrinic.tal/$f/roas.csv -O /tmp/afrinic-roas.csv
wget https://ftp.ripe.net/rpki/arin.tal/$f/roas.csv -O /tmp/arin-roas.csv
wget https://ftp.ripe.net/rpki/lacnic.tal/$f/roas.csv -O /tmp/lacnic-roas.csv
wget https://ftp.ripe.net/rpki/ripencc.tal/$f/roas.csv -O /tmp/ripencc-roas.csv

cat /tmp/apnic-roas.csv >> vrps/vrps-$d.csv
tail -n +2 /tmp/afrinic-roas.csv >> vrps/vrps-$d.csv
tail -n +2 /tmp/arin-roas.csv >> vrps/vrps-$d.csv
tail -n +2 /tmp/lacnic-roas.csv >> vrps/vrps-$d.csv
tail -n +2 /tmp/ripencc-roas.csv >> vrps/vrps-$d.csv


rm /tmp/apnic-roas.csv >> vrps/vrps-$d.csv
rm /tmp/afrinic-roas.csv >> vrps/vrps-$d.csv
rm /tmp/arin-roas.csv >> vrps/vrps-$d.csv
rm /tmp/lacnic-roas.csv >> vrps/vrps-$d.csv
rm /tmp/ripencc-roas.csv >> vrps/vrps-$d.csv
