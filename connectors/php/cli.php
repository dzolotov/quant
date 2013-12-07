<?php

date_default_timezone_set('Europe/Moscow');
error_reporting(E_ALL);
ini_set('display_errors', '1');
mb_internal_encoding("UTF-8");
declare(ticks = 1);


require_once("proton.php");
require_once('log4php/Logger.php');
require_once('ColoredConsoleAppender.php');
require_once('Request.php');
require_once('SharedMemoryWrapper.php');


define('QUEUE_NO_MESSAGES', '[-7]: ');
define('QUEUE_NO_SOURCES', '[-5]: no valid sources');


define("PUBSUB_SERVICE_CHANNEL_ID", "service_channel");
define('PUBSUB_HOST', 'forms.herzen.spb.ru');


define('AMQP_HOST', '10.0.16.240');
define('AMQP_PORT', '5672');
define('AMQP_USER', 'admin');
define('AMQP_PASS', 'admin');

define('ENDPOINT_NAME', "endpoint_forms");
define('AMQP_CONNECTION_STRING', "amqp://" . AMQP_USER . ":" . AMQP_PASS . "@" . AMQP_HOST . ":" . AMQP_PORT . "/");

define('EXCHANGE_UNO', AMQP_CONNECTION_STRING . "uno");
define('EXCHANGE_INBOUND', AMQP_CONNECTION_STRING . ENDPOINT_NAME);
define('QUEUE_INBOUND', AMQP_CONNECTION_STRING . ENDPOINT_NAME);


function create_uuid() {
    return sprintf('%04x%04x-%04x-%04x-%04x-%04x%04x%04x',

    // 32 bits for "time_low"
    mt_rand(0, 0xffff), mt_rand(0, 0xffff),

    // 16 bits for "time_mid"
    mt_rand(0, 0xffff),

    // 16 bits for "time_hi_and_version",
    // four most significant bits holds version number 4
    mt_rand(0, 0x0fff) | 0x4000,

    // 16 bits, 8 bits for "clk_seq_hi_res",
    // 8 bits for "clk_seq_low",
    // two most significant bits holds zero and one for variant DCE1.1
    mt_rand(0, 0x3fff) | 0x8000,

    // 48 bits for "node"
    mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff)
    );
}

function sendTextMessage($logger, &$messenger, $address, $subject, $body, $properties = array()) {
    if (empty($body)) {
        $body = "dummy body for #" . getmypid();
    }

    $logger->info("TM:[subject:$subject][address:$address][body:$body][properties:" .json_encode($properties) . "]");

    try {
        $msg = new Message();
        $msg->subject = $subject;
        $msg->address = $address;
        $msg->reply_to = ENDPOINT_NAME;
        // $msg->durable = true;
        $msg->body = $body;
        $msg->annotations = array("dummy_string");
        $msg->properties = $properties;

        $messenger->put($msg);
        $messenger->send();

        // $messenger->stop();
        // $messenger->start();

        $outgoing = $messenger->outgoing;
        $incoming = $messenger->incoming;

        $logger->info("Sent '$subject' message to '$address', depth: " . $outgoing . ":" . $incoming);

        // var_dump($msg);
        // var_dump($messenger);

    } catch (Exception $e) {
        $logger->warn("Exception in '$subject' message send: " . $e->getCode() . ":" . $e->getMessage());
    }
}

function sendEchoMessage($logger, &$messenger, $address, $properties = array()) {

    sendTextMessage($logger, $messenger, $address, "echo." . ENDPOINT_NAME, '', $properties);
}

function sendHelloMessage($logger, &$messenger, $address, $msg = null) {

    $schema = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            . '<schema>'
                . '<class id="employee" />'
                . '<class id="person" />'
                . '<class id="student" />'
            . '</schema>';

    sendTextMessage($logger, $messenger, $address, "hello." . ENDPOINT_NAME, $schema);
}

function sendByeMessage($logger, &$messenger, $address) {

    sendTextMessage($logger, $messenger, $address, "bye." . ENDPOINT_NAME, '');
}

function sendPongMessage($logger, &$messenger, $address, $msg) {

    $properties = array();
    $transactionId = isset($msg->properties) && isset($msg->properties["transactionId"]) ? $msg->properties["transactionId"] : null;
    if (!empty($transactionId)) {
        $properties["transactionId"] = $transactionId;
    }
    sendTextMessage($logger, $messenger, $address, "pong." . ENDPOINT_NAME, '', $properties);
}

function sendGetMessage($logger, &$messenger, $address, $class, $uuid) {

    $messageId = create_uuid();

    $properties = array();
    $properties["transactionId"] = create_uuid();
    $properties["messageId"] = $messageId;
    $properties["uuid"] = $uuid;

    $logger->info("GM:[subject:get." . $class . "." . $uuid . "][address:$address][properties:" .json_encode($properties) . "]");

    sendTextMessage($logger, $messenger, $address, "get." . $class, '', $properties);

    return $messageId;
}

$shutdown_hook = function ($logger, &$messenger, $children_pids = array(), $bye = false) {
    foreach ($children_pids as $child_pid) {
        $child_killed = posix_kill($child_pid, SIGINT);
        $logger->info("Killed child #$child_pid: " . (int)$child_killed);
    }

    $logger->info("Stop messenger in " . getmypid());

    try {
        if (!is_null($messenger)) {
            if ($bye) {
                sendByeMessage($logger, $messenger, EXCHANGE_UNO);
            }
            $messenger->stop();
            $messenger = null;
        }

        $logger->debug("Stopped messenger");

    } catch (Exception $e) {
        $logger->warn("Exception in shutdown function for process #" . getmypid() . ": " . $e->getCode() . ', ' . $e->getMessage());
        $logger->warn($e->getTraceAsString());
    }

    $logger->info("Stopped process #" . getmypid());
};

$sigint = function ()  {
    exit;
};



Logger::configure('config.xml');











////////////////////////
//
//  M A I N
//
////////////////////////

$cli_logger = Logger::getLogger("cli");
$cli_logger->info("cli logger init");

$cli_logger->info("Started CLI: pid: #" . getmypid());

$cli_mess = new Messenger();
$cli_mess->subscribe(QUEUE_INBOUND);
$cli_mess->start();
$cli_mess->incoming_window = 1;
$cli_mess->outgoing_window = 1;
$cli_mess->timeout = 100;


pcntl_signal(SIGINT, $sigint);
pcntl_signal(SIGTERM, $sigint);
register_shutdown_function($shutdown_hook, $cli_logger, $cli_mess, array(), true);



$is_send_activated = false;
$echoMessageId = create_uuid();

$i = 0;
while (true) {
    $cli_logger->info("Self-test iteration #$i...");
    $i++;

    try {
        $cli_mess->recv(1);

        $msg = new Message();
        $cli_mess->get($msg);

        $tracker = $cli_mess->incoming_tracker();

        $subject = $msg->subject;
        $properties = $msg->properties;
        $messageId = isset($msg->properties["messageId"]) ? $msg->properties["messageId"] : null;

        if (!empty($subject) && !empty($messageId) && $subject == "echo." . ENDPOINT_NAME && $messageId == $echoMessageId) {

            $cli_logger->info("ECHO FOUND: $subject");
            $is_send_activated = true;
            $cli_mess->timeout = -1;

            // sendByeMessage($cli_logger, $cli_mess, EXCHANGE_UNO);
            sendHelloMessage($cli_logger, $cli_mess, EXCHANGE_UNO);

            $cli_mess->accept($tracker);
            break;
        }


        $cli_logger->info("Found message (" . ($msg->subject ? $msg->subject : "<no subject>") . ") -> ignore");
        $cli_mess->accept($tracker);

        $cli_logger->info("->");

    } catch (Exception $e) {

        $exceptionMessage = $e->getMessage();

        $cli_logger->warn("Exception in self-test loop: " . $e->getCode() . ":" . $e->getMessage());

        if ($exceptionMessage == QUEUE_NO_MESSAGES) {
            sendEchoMessage($cli_logger, $cli_mess, EXCHANGE_INBOUND, array("messageId" => $echoMessageId));
            continue;
        }

        if ($exceptionMessage == QUEUE_NO_SOURCES) {
            $cli_logger->error("No queue server - exit");
            break;
        }

        $cli_logger->warn($e->getTraceAsString());

        sleep(1);
    }
}

if (!$is_send_activated) {
    $cli_logger->info("Send not activated - exit");
    exit;
}


$cli_logger->info("Send activated, now fork");


$pid = pcntl_fork();
if ($pid == -1) {
    $cli_logger->error('could not fork');
    exit;
} else if ($pid) {
    // we are the parent

    $receiver_logger = Logger::getLogger("receiver");
    $receiver_logger->addAppender(new ColoredConsoleAppender("receiver", null, "cyan"));
    $receiver_logger->info("receiver logger init");

    $children_pids = array();
    $children_pids[] = $pid;
    $parent_pid = getmypid();

    $receiver_mess = new Messenger();
    $receiver_mess->subscribe(QUEUE_INBOUND);
    $receiver_mess->start();
    $receiver_mess->incoming_window = 1;
    $receiver_mess->outgoing_window = 1;
    $receiver_mess->timeout = -1;

    pcntl_signal(SIGINT, $sigint);
    pcntl_signal(SIGTERM, $sigint);
    register_shutdown_function($shutdown_hook, $receiver_logger, $receiver_mess, $children_pids, true);



    $receiver_logger->info("Message Loop process initialized, pid: " . $parent_pid);

    $receiver_logger->info("Waiting for messages...");

    $i = 0;
    while (true) {
        $receiver_logger->info("Message loop iteration #$i...");
        $i++;

        try {
            $receiver_mess->recv(1);

            $msg = new Message();
            $receiver_mess->get($msg);
            // var_dump($msg);
            var_dump($msg->properties);

            $tracker = $receiver_mess->incoming_tracker();

            $receiver_logger->info("Found new message:");

            $subject = $msg->subject;
            if (empty($subject)) {
                $receiver_logger->warn("Empty message subject: reject");
                $receiver_mess->accept($tracker);
                continue;
            }

            $receiver_logger->info("Message subject: " . $subject);

            $subject_parts = explode(".", $subject);
            $action = isset($subject_parts[0]) ? $subject_parts[0] : null;
            $class = isset($subject_parts[1]) ? $subject_parts[1] : null;
            $uuid = isset($subject_parts[2]) ? $subject_parts[2] : null;

            $body = $msg->body;

            if (empty($action)) {
                $receiver_logger->warn("Empty message action: reject");
                $receiver_mess->accept($tracker);
                continue;
            }

            switch ($action) {
                case 'discover':
                    $receiver_logger->info("DISCOVER FOUND");
                    // send hello message

                    sendHelloMessage($receiver_logger, $receiver_mess, EXCHANGE_UNO, $msg);
                    $receiver_mess->accept($tracker);

                    break;

                case 'ping':
                    $receiver_logger->info("PING FOUND");
                    // send pong message

                    sendPongMessage($receiver_logger, $receiver_mess, EXCHANGE_UNO, $msg);
                    $receiver_mess->accept($tracker);

                    break;


                default:
                    $messageId = isset($msg->properties["messageId"]) ? $msg->properties["messageId"] : null;

                    if (!empty($messageId)) {
                        $receiver_logger->info("messageId for '$subject': $messageId");
                        $requestId = SharedMemoryWrapper::get($messageId);
                        if (!empty($requestId)) {
                            SharedMemoryWrapper::delete($messageId);
                            $receiver_logger->info("Found request in shared memory: #$requestId");

                            $postRq = new Request("forms.herzen.spb.ru", 80, "/pub/?id=" . $requestId);
                            $postRq->header("Accept", "application/json");
                            $postRq->header("Connection", "close");
                            $postResponse = $postRq->post($body);
                            $receiver_logger->info("Send message body to frontend: \n" . $body);
                            break;
                        } else {
                            $receiver_logger->info("Request not found in shared memory (messageId $messageId): #$requestId");
                        }

                    } else {

                        $receiver_logger->info("messageId is empty");
                    }

                    $receiver_logger->info("Ignore message '$subject'");
                    $receiver_mess->accept($tracker);

                    break;
            }

        } catch (Exception $e) {

            $exceptionMessage = $e->getMessage();

            $receiver_logger->warn("Exception in message loop: " . $e->getCode() . ":" . $e->getMessage());
            $receiver_logger->warn($e->getTraceAsString());

            switch ($exceptionMessage) {
                case QUEUE_NO_SOURCES:
                    $receiver_logger->error("No queue server - exit");
                    exit();
                    break;

                default:
                    # code...
                    break;
            }

            sleep(2);
            continue;
        }

        $receiver_logger->info("->");
    }

    pcntl_wait($status); //Protect against Zombie children

    $receiver_logger->info("Execution finished for #" . $parent_pid);

} else {
    // we are the child
    $requester_logger = Logger::getLogger("sender");
    $requester_logger->addAppender(new ColoredConsoleAppender("sender", "black", "yellow"));
    $requester_logger->info("sender logger init");

    $requester_mess = new Messenger();
    $requester_mess->start();
    $requester_mess->outgoing_window = 1;
    $requester_mess->timeout = -1;


    pcntl_signal(SIGINT, $sigint);
    pcntl_signal(SIGTERM, $sigint);
    register_shutdown_function($shutdown_hook, $requester_logger, $requester_mess, array(), true);



    $requester_logger->info("Requester process init, pid: " . getmypid());

    $processedRequests = array();

    $j = 0;
    while (true) {
        $requester_logger->info("Requester iteration #$j...");
        $j++;

        $requester_logger->info("Request started");
        try {
            $getRq = new Request(PUBSUB_HOST, 80, "/sub/" . PUBSUB_SERVICE_CHANNEL_ID);
            $getRqResponse = $getRq->get();
        } catch (Exception $e) {
            $requester_logger->warn("Exception in sub request: " . $e->getCode() . ":" . $e->getMessage());
            $requester_logger->warn($e->getTraceAsString());
        }
        $requester_logger->info("Request finished");
        if (empty($getRqResponse)) {
            $requester_logger->info("Empty request");
            sleep(2);
            continue;
        }
        $requests = explode("\n", trim($getRqResponse));

        if (empty($requests)) {
            $processedRequests = array();
        }
        $requestMessageDelimiter = " ";
        foreach ($requests as $requestMessageText) {
            $requestMessageText = trim($requestMessageText);
            $requestMessageDelimiterPos = strpos($requestMessageText, $requestMessageDelimiter);
            $requestId = substr($requestMessageText, 0, $requestMessageDelimiterPos);

            // processed?
            if (in_array($requestId, $processedRequests)) {
                $requester_logger->info("Message #$requestId was already processed");
                sleep(2);
                continue;
            }

            $requestMessageContent = substr($requestMessageText, $requestMessageDelimiterPos+1);

            $requestMessage = json_decode($requestMessageContent, true);

            $requester_logger->info("Have message #$requestId: " . $requestMessageContent);


            // process

            if (is_null($requestMessage)) {
                $requester_logger->info("Message #$requestId is empty");

            } else {
                $messageAction = $requestMessage["action"];

                $requester_logger->info("Message #$requestId is '$messageAction' message");

                switch ($messageAction) {
                    case 'get':
                        $class = $requestMessage["class"];
                        $uuid = $requestMessage["uuid"];

                        $requester_logger->info("Message #$requestId has action '$messageAction', class $class and uuid $uuid");

                        try {

                            $messageId = sendGetMessage($requester_logger, $requester_mess, EXCHANGE_UNO, $class, $uuid);
                            SharedMemoryWrapper::set($messageId, $requestId);
                            $requester_logger->info("Message #$requestId $messageAction.$class.$uuid sent and saved in memory $messageId -> $requestId");
                            $testRequestId = SharedMemoryWrapper::get($messageId);
                            $requester_logger->info("Message #$requestId shmop test: $messageId -> $testRequestId)");


                        } catch (Exception $e) {
                            $requester_logger->info("Request #$requestId wasn't saved in shared memory");
                        }

                        break;

                    default:
                        $requester_logger->info("Ignore message #$requestId");
                        break;
                }


            }

            $processedRequests[] = $requestId;
        }

        // var_dump($requests, $getRqResponse);
        // sleep(2);

        $requester_logger->info("->");
    }


    $requester_logger->info("Execution finished for #" . getmypid());
}


$cli_logger->info("Stopping #" . getmypid());
