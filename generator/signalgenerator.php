<?php
require_once __DIR__ . "/vendor/autoload.php";

$client = new Predis\Client('tcp://127.0.0.1:6379');

$authors = explode(',', 'adam,john,bob,lucy,sue');
$resorts = explode(',', 'android,xbox,apple');

for ($i = 1; $i < 20; $i++) {
    $client->publish("URLS", json_encode([
        'url'=>"http://example.com/page/{$i}]",
        'author'=>$authors[array_rand($authors, 1)],
        'resort'=>$resorts[array_rand($resorts, 1)],
        'factor'=>rand(1,10)/10,
    ]));
    sleep(1);
}
