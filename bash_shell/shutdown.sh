for i in root@hadoop101 root@hadoop102 root@hadoop100
do 
 ssh $i 'shutdown -P now'
done
