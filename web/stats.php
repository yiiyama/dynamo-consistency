
<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

date_default_timezone_set('America/New_York');

$db = new SQLite3('stats.db');
$table = 'stats';
$condition = '';
$join = '';

if (isset($_GET['history'])) {

  $site = $_GET['history'];
  $condition = ' WHERE site = "' . $site . '"';
  $table = $table . $condition . ' UNION SELECT * FROM ' . $table . '_history';

} else {

  $which_sites = isset($_GET['sites']) ? $_GET['sites'] : 'good';

  if($which_sites != 'all') {

    $join = ' INNER JOIN sites on sites.site = ' . $table . '.site';
    $isgood = ($which_sites == 'need_checked' || $which_sites == 'bad') ? '0' : '1';
    $condition = ' WHERE isgood = ' . $isgood;

  }
}

$results = $db->query('SELECT * FROM ' .
                      $table . $join . $condition .
                      ' ORDER BY site ASC, entered DESC');

$config = json_decode(file_get_contents('consistency_config.json'), true);

include 'output.html';

$db = null;
?>
