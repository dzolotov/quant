<?php

error_reporting(E_ALL);
ini_set('display_errors', '1');
mb_internal_encoding("UTF-8");

define("SERVICE_CHANNEL_ID", "service_channel");

require_once('Request.php');


$response = array();

$subId = uniqid();

$response["subId"] = $subId;

$body = $subId . " " . json_encode(array(
        "action" => $_GET["action"]
        , "class" => $_GET["class"]
        , "uuid" => $_GET["uuid"]
    ));

$postRq = new Request("forms.herzen.spb.ru", 80, "/pub/?id=" . SERVICE_CHANNEL_ID);
$postRq->header("Accept", "application/json");
$postRq->header("Connection", "close");
$postResponse = $postRq->post($body);

$response["stats"] = json_decode($postResponse);


header("Content-Type: application/json");
print json_encode($response);

