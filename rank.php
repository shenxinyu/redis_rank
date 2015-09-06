<?php
set_time_limit(0);
ini_set('memory_limit', '20480M');

$tstart = microtime(true);
echo "start job, time[$tstart]", PHP_EOL;

$redis = new Redis();
$redis->connect('/tmp/redis.sock');
$pipeline = $redis->pipeline();
//$redis->connect('localhost');
//$redis->del('rank-1', 'rank-2', 'rank-3');

//import data to redis from database
$tb = microtime(true);
$step = 1000000;
$db = new mysqli('127.0.0.1', 'root', '', 'friend');
//$db = new mysqli('119.254.111.25', 'myroot', 'jXIN@274.com', 'friend', 60002);
$sql = "SELECT COUNT(*) FROM friend";
if ($result = $db->query($sql)) {
    $row = $result->fetch_row();
    $num  = $row[0];
}
$i = 0;
for ($j=0; $j*$step < $num; $j++) {
$sql = 'SELECT uid, fid from friend limit '.($j*$step).', '.min($step, $num-$j*$step);
if ($result = $db->query($sql)) {
    while ($row = $result->fetch_assoc()) {
        $pipeline->sAdd($row['uid'], $row['fid']);

        //record user set 
        $pipeline->sAdd('user_set', $row['uid']);
        if ($i%10000 == 0) {
            echo '-';
        }
        $i++;
    }
    $result->free();
    $pipeline->exec();
}
}
$db->close();
echo $i, PHP_EOL;
$te = microtime(true);
$cost = $te - $tb;
echo 'import data cost: '.$cost, PHP_EOL;

//$len = $redis->sCard('user_set');
$userset = $redis->sMembers('user_set');
$len = count($userset);
echo 'user count: '.$len, PHP_EOL;

//generate 1-dim ran
$i = 0;
$tb = microtime(true);
foreach($userset as $uid) {
    $pipeline->sCard($uid);
    if ($i%10000 == 0) {
        echo '*';
    }
    $i++;
}
$tempArray = $pipeline->exec();

$i = 0;
foreach($userset as $index => $uid) {
    $redis->zAdd('rank-1', $tempArray[$index], $uid);
    if ($i%10000 == 0) {
        echo '.';
    }
    $i++;
}
echo PHP_EOL;

$te = microtime(true);
$cost = $te - $tb;
echo 'generate 1-dim rank cost: '.$cost, PHP_EOL;

//generate 2-dim rank
$tb = microtime(true);
$i = 0;
foreach ($userset as $uid) {
    $pipeline->sMembers($uid);
    if ($i%10000 == 0) {
        echo '*';
    }
    $i++;
}
$tempArray = $pipeline->exec();

$i = 0;
foreach ($userset as $index => $uid) {
    $par = implode('","', $tempArray[$index]);
    $str = '$pipeline->sUnionStore("'.$uid.'-2dim", "'.$par.'");';
    eval($str);
    $pipeline->sDiffStore($uid.'-2dim', $uid.'-2dim', $uid);
    $pipeline->sRem($uid.'-2dim', $uid);
    $pipeline->sCard($uid.'-2dim');
    if ($i%10000 == 0) {
        echo '*';
    }
    $i++;
}
$dim2Array = $pipeline->exec();

$i = 0;
foreach ($userset as $index => $uid) {
    $pipeline->zAdd('rank-2', $dim2Array[$index], $uid);
    if ($i%10000 == 0) {
        echo '.';
    }
    $i++;
}
echo PHP_EOL;

$te = microtime(true);
$cost = $te - $tb;
echo 'generate 2-dim rank cost: '.$cost, PHP_EOL;

//generate 3-dim rank
$tb = microtime(true);
$i = 0;
foreach ($userset as $index => $uid) {
    $par = implode('","', $dim2Array[$index]);
    $str = '$pipeline->sUnionStore("'.$uid.'-3dim", "'.$par.'");';
    eval($str);
    $pipeline->sDiffStore($uid.'-3dim', $uid.'-3dim', $uid.'-2dim', $uid);
    $pipeline->sRem($uid.'-3dim', $uid);
    $pipeline->sCard($uid.'-3dim');
    if ($i%10000 == 0) {
        echo '*';
    }
    $i++;
}
$tempArray = $pipeline->exec();

$i = 0;
foreach ($userset as $index => $uid) {
    $pipeline->zAdd('rank-3', $tempArray[$index], $uid);

    //free memory
    $pipeline->del($uid.'-3dim');
    if ($i%10000 == 0) {
        echo '.';
    }
    $i++;
}
echo PHP_EOL;

$te = microtime(true);
$cost = $te - $tb;
echo 'generate 3-dim rank cost: '.$cost, PHP_EOL;

$tend = microtime(true);
$cost = $tend - $tstart;
echo 'total cost: '.$cost, PHP_EOL;
var_dump($redis->zRevRange('rank-1', 0, 9, true));
var_dump($redis->zRevRange('rank-2', 0, 9, true));
var_dump($redis->zRevRange('rank-3', 0, 9, true));

$redis->close();
