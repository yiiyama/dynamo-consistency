
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

  $site = SQLite3::escapeString($_GET['history']);

  // Check that $site is really a site
  $invalid_site = true;
  $check_result = $db->query('SELECT site FROM sites');
  while ($check = $check_result->fetchArray()) {
    if ($check[0] == $site) {
      $invalid_site = false;
      break;
    }
  }

  if ($invalid_site) {
    $db = null;
    die('Invalid site name');
  }

  $condition = ' WHERE site = \'' . $site . '\'';
  $table = $table . $condition . ' UNION SELECT * FROM ' . $table . '_history';

} else {

  $which_sites = isset($_GET['sites']) ? $_GET['sites'] : 'good';
  $join = ' INNER JOIN sites on sites.site = ' . $table . '.site';

  if($which_sites != 'all') {

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
