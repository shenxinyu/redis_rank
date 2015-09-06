<?php
ini_set('memory_limit', '10240M');

class Rank {
var $redirect_stdout = false;
var $workers = [];
var $worker_num = 10;
var $data_row = 0;
var $status = 'import';
var $start_time = 0;
var $middle_time = 0;

var $remote_redis_ip='localhost';
var $remote_redis_port=6379;
var $local_redis_ip='localhost';

function run() {
//swoole_process::daemon(0, 1);
$this->start_time = $this->middle_time = microtime(true);
if ($this->status == 'import') {
    $db = new mysqli('127.0.0.1', 'root', '', 'friend');
    //$db = new mysqli('119.254.111.25', 'myroot', 'jXIN@274.com', 'friend', 60002);
    $sql = "SELECT COUNT(*) FROM friend";
    if ($result = $db->query($sql)) {
        $row = $result->fetch_row();
        $this->data_row = $row[0];
        $result->free();
    }
    $db->close();
    echo 'total data count: '.$this->data_row.PHP_EOL;
}
//$this->data_row = 100000;

for($i = 0; $i < $this->worker_num; $i++)
{
    $process = new swoole_process(array($this, 'child_async'), $this->redirect_stdout);
    $process->id = $i;
    $pid = $process->start();
    $this->workers[$pid] = $process;
    //echo "Master: new worker, PID=".$pid."\n";
}

$this->master_async();
}

//异步主进程
function master_async()
{
    $count = 0;
    swoole_process::signal(SIGCHLD, function ($signo) use (&$count) {
        while(1)
        {
            $ret = swoole_process::wait(false);
            if ($ret)
            {
                $pid = $ret['pid'];
                $count++;
                //echo "Worker Exit, kill_signal={$ret['signal']} PID=" . $pid . PHP_EOL;
                //echo "Count=" . $count . " Worker_num=" . $this->worker_num . " Stauts=" . $this->status . PHP_EOL;
                if ($count == $this->worker_num) {
                    switch ($this->status) {
                    case '3-dim':
                        $this->report_cost();
                        exit(0);
                        break;
                    case 'import':
                        $this->report_cost();
                        $this->status = '1-dim';
                        $this->restart_process();
                        break;
                    case '1-dim':
                        $this->report_cost();
                        $this->status = '2-dim';
                        $this->restart_process();
                        break;
                    case '2-dim':
                        $this->report_cost();
                        $this->status = '3-dim';
                        $this->restart_process();
                        break;
                    default:
                        break;
                    }
                    $count = 0;
                }
            }
            else
            {
                break;
            }
        }
    });

    /**
     * @var $process swoole_process
     */
    foreach($this->workers as $pid => $process)
    {
        swoole_event_add($process->pipe, function($pipe) use ($process) {
            $recv = $process->read();
            if ($recv) echo "From Worker: " . $recv;
            $process->write("HELLO worker {$process->pid}\n");
        });
        $process->write("hello worker[$pid]\n");
    }
}

function restart_process() {
    foreach ($this->workers as $pid => $child_process) {
        $new_pid = $child_process->start();
        $this->workers[$new_pid] = $child_process;
        unset($this->workers[$pid]);
    }
}

function report_cost() {
    $end_time = microtime(true);
    $cost = $end_time - $this->middle_time;
    echo PHP_EOL;
    echo 'step '.$this->status.' cost: '.$cost, PHP_EOL;
    $this->middle_time = $end_time;
    $redis = new Redis();
    //$redis->connect('/tmp/redis.sock');
    $redis->connect($this->remote_redis_ip);
    switch ($this->status) {
    case '1-dim':
        var_dump($redis->zRevRange('rank-1', 0, 9, true));
        break;
    case '2-dim';
        var_dump($redis->zRevRange('rank-2', 0, 9, true));
        break;
    case '3-dim':
        var_dump($redis->zRevRange('rank-3', 0, 9, true));
        break;
    }

    if ($this->status == '3-dim') {
        $cost = $end_time - $this->start_time;
        echo 'total cost: '.$cost, PHP_EOL;
    }
}

function child_async(swoole_process $worker)
{
    //echo "Worker: start. PID=".$worker->pid."\n";
    global $argv;
    $worker->name("{$argv[0]}: worker #".$worker->id."/".$this->worker_num);

    swoole_process::signal(SIGTERM, function($signal_num) use ($worker) {
		echo "signal call = $signal_num, #{$worker->pid}\n";
    });

    switch ($this->status) {
        case 'import':
            echo " worker #".$worker->id."/".$this->worker_num." enter status: ".$this->status.PHP_EOL;
            $this->import_data($worker);
            break;
        case '1-dim':
            echo " worker #".$worker->id."/".$this->worker_num." enter status: ".$this->status.PHP_EOL;
            $this->generate_1dim_rank($worker);
            break;
        case '2-dim':
            echo " worker #".$worker->id."/".$this->worker_num." enter status: ".$this->status.PHP_EOL;
            $this->generate_2dim_rank($worker);
            break;
        case '3-dim':
            echo " worker #".$worker->id."/".$this->worker_num." enter status: ".$this->status.PHP_EOL;
            $this->generate_3dim_rank($worker);
            break;
        default:
            break;
    }
    $worker->exit(0);
}

function import_data($worker) {
    $redis = new Redis();
    //$redis->connect('/tmp/redis.sock');
    $redis->connect($this->remote_redis_ip);
    $local_redis = $local_pipeline = array();
    for ($i=0; $i < $this->worker_num; $i++) {
        $local_redis[] = $lr = new Redis();
        $lr->connect($this->local_redis_ip, $this->remote_redis_port+$i+1);
    }

    $num = ceil((float)$this->data_row/$this->worker_num);
    $start = $worker->id*$num;
    $i = 0;
    $db = new mysqli('127.0.0.1', 'root', '', 'friend');
    $step = 1000000;
    //$db = new mysqli('119.254.111.25', 'myroot', 'jXIN@274.com', 'friend', 60002);
    for ($j=0; $j*$step < $num; $j++) {
    $sql = 'SELECT uid, fid from friend limit '.($start+$j*$step).', '.min($step, $num-$j*$step);
    //$sql = 'SELECT uid, fid from friend limit '.$start.', '.$num;
    if ($result = $db->query($sql)) {
        $pipeline = $redis->pipeline();
        foreach ($local_redis as $lr) {
            $local_pipeline[] = $lr->pipeline();
        }
        while ($row = $result->fetch_assoc()) {
            $pipeline->sAdd($row['uid'], $row['fid']);
            foreach ($local_pipeline as $lp) {
                $lp->sAdd($row['uid'], $row['fid']);
            }

            //record userid list 
            $pipeline->sAdd('user_set', $row['uid']);
            if ($i%10000 == 0) {
                echo '-';
            }
            $i++;
        }
        $result->free();
        $pipeline->exec();
        foreach ($local_pipeline as $lp) {
            $lp->exec();
        }
    }
    }

    $db->close();
    $redis->close();
    foreach ($local_redis as $lr) {
        $lr->close();
    }
}

function generate_1dim_rank($worker) {
    $redis = new Redis();
    //$redis->connect('/tmp/redis.sock');
    $redis->connect($this->remote_redis_ip);
    $local_redis = new Redis();
    $local_redis->connect($this->local_redis_ip, $this->remote_redis_port+$worker->id+1);

    $userlist = $redis->sMembers('user_set');
    $len = count($userlist);
    //$len = $redis->sCard('user_set');
    $num = ceil((float)$len/$this->worker_num);
    $userlist = array_slice($userlist, $worker->id*$num, $num);

    $local_pipeline = $local_redis->pipeline();
    foreach ($userlist as $uid) {
       $local_pipeline->sAdd('user_set', $uid);
    }
    $local_pipeline->exec();


    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $uid) {
        $local_pipeline->sCard($uid);
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $tempArray = $local_pipeline->exec();

    $pipeline = $redis->pipeline();
    $i = 0;
    foreach ($userlist as $index => $uid) {
        $pipeline->zAdd('rank-1', $tempArray[$index], $uid);
        if ($i%10000 == 0) {
            echo '.';
        }
        $i++;
    }
    $pipeline->exec();

    $redis->close();
}

function generate_2dim_rank($worker) {
    $redis = new Redis();
    //$redis->connect('/tmp/redis.sock');
    $redis->connect($this->remote_redis_ip);
    $local_redis = new Redis();
    $local_redis->connect($this->local_redis_ip, $this->remote_redis_port+$worker->id+1);

/*
    $userlist = $redis->sMembers('user_set');
    $len = count($userlist);
    //$len = $redis->sCard('user_set');
    $num = ceil((float)$len/$this->worker_num);
    $userlist = array_slice($userlist, $worker->id*$num, $num);
*/
    $userlist = $local_redis->sMembers('user_set');
    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $uid) {
        $local_pipeline->sMembers($uid);
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $tempArray = $local_pipeline->exec();

    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $index => $uid) {
        $par = implode('","', $tempArray[$index]);
        $str = '$local_pipeline->sUnionStore("'.$uid.'-2dim", "'.$par.'");';
        eval($str);
        $local_pipeline->sDiffStore($uid.'-2dim', $uid.'-2dim', $uid);
        $local_pipeline->sRem($uid.'-2dim', $uid);
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $local_pipeline->exec();

    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $uid) {
        $local_pipeline->sCard($uid.'-2dim');
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $tempArray = $local_pipeline->exec();

    $pipeline = $redis->pipeline();
    $i = 0;
    foreach ($userlist as $index => $uid) {
        $pipeline->zAdd('rank-2', $tempArray[$index], $uid);
        if ($i%10000 == 0) {
            echo '.';
        }
        $i++;
    }
    $pipeline->exec();

    $local_redis->close();
    $redis->close();
}

function generate_3dim_rank($worker) {
    $redis = new Redis();
    //$redis->connect('/tmp/redis.sock');
    $redis->connect($this->remote_redis_ip);
    $local_redis = new Redis();
    $local_redis->connect($this->local_redis_ip, $this->remote_redis_port+$worker->id+1);

/*
    $userlist = $redis->sMembers('user_set');
    $len = count($userlist);
    //$len = $redis->sCard('user_set');
    $num = ceil((float)$len/$this->worker_num);
    $user_list = array_slice($userlist, $worker->id*$num, $num);
*/
    $user_list = $local_redis->sMembers('user_set');
    $num = count($user_list);

    $step = 50000;
    for ($j=0;$j*$step < $num; $j++) {
    $userlist = array_slice($user_list, $j*$step, $step);
    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $uid) {
        $local_pipeline->sMembers($uid.'-2dim');
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $tempArray = $local_pipeline->exec();

    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $index => $uid) {
        $par = implode('","', $tempArray[$index]);
        $str = '$local_pipeline->sUnionStore("'.$uid.'-3dim", "'.$par.'");';
        eval($str);
        $local_pipeline->sDiffStore($uid.'-3dim', $uid.'-3dim', $uid.'-2dim', $uid);
        $local_pipeline->sRem($uid.'-3dim', $uid);
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $local_pipeline->exec();

    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $index => $uid) {
        $local_pipeline->sCard($uid.'-3dim');
        if ($i%10000 == 0) {
            echo '*';
        }
        $i++;
    }
    $tempArray = $local_pipeline->exec();

    $pipeline = $redis->pipeline();
    $local_pipeline = $local_redis->pipeline();
    $i = 0;
    foreach ($userlist as $index => $uid) {
        $pipeline->zAdd('rank-3', $tempArray[$index], $uid);

        //free memory
        $local_pipeline->del($uid.'-3dim');
        if ($i%10000 == 0) {
            echo '.';
        }
        $i++;
    }
    $pipeline->exec();
    $local_pipeline->exec();
    }

    $local_redis->close();
    $redis->close();
}

}

$rank = new Rank();
$rank->run();
