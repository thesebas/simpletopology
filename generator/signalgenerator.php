<?php
require_once __DIR__ . "/vendor/autoload.php";

$client = new \Predis\Client('tcp://127.0.0.1:6379');

$authors = explode(',', 'adam,john,bob,lucy,sue');
$resorts = explode(',', 'android,xbox,apple');

for ($i = 1; $i < 30; $i++) {
    echo $i, PHP_EOL;
    $resort = $resorts[array_rand($resorts, 1)];
    $id = rand(1, 10);
    $msg = json_encode([
        'url' => "http://example.com/{$resort}/article-id-{$id}",
        'author' => $authors[array_rand($authors, 1)],
        'resort' => $resort,
        'factor' => rand(1, 10) / 10,
    ]);
    $client->publish("URLS", $msg);
}
