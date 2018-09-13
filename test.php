<?php 

namespace Test;

require_once('vendor/autoload.php');
require_once('src/mysql.php');

//print_r(hash_algos());


//use \Net\Mysql;

$loop = \React\EventLoop\Factory::create();

$params = array(
  'debug' => true
  ,'hostname' => 'localhost'
);

echo "mysql = new \Net\Mysql(params,loop).".PHP_EOL;
$mysql = new \Net\Mysql($params,$loop);
echo "Starting loop.".PHP_EOL;
$loop->run();

?>