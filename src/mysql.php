<?php 

namespace Net;

use \Evenement\EventEmitter;

class Mysql extends EventEmitter
{

	const STATE_CONNECT  = 'connecting';
	const STATE_AUTH     = 'authenticating';
	const STATE_READY    = 'ready';

	const COMMAND_SLEEP          = "\x00";
	const COMMAND_QUIT           = "\x01";
	const COMMAND_INIT_DB        = "\x02";
	const COMMAND_QUERY          = "\x03";
	const COMMAND_FIELD_LIST     = "\x04";
	const COMMAND_CREATE_DB      = "\x05";
	const COMMAND_DROP_DB        = "\x06";
	const COMMAND_REFRESH        = "\x07";
	const COMMAND_SHUTDOWN       = "\x08";
	const COMMAND_STATISTICS     = "\x09";
	const COMMAND_PROCESS_INFO   = "\x0A";
	const COMMAND_CONNECT        = "\x0B";
	const COMMAND_PROCESS_KILL   = "\x0C";
	const COMMAND_DEBUG          = "\x0D";
	const COMMAND_PING           = "\x0E";
	const COMMAND_TIME           = "\x0F";
	const COMMAND_DELAYED_INSERT = "\x10";
	const COMMAND_CHANGE_USER    = "\x11";
	const COMMAND_BINLOG_DUMP    = "\x12";
	const COMMAND_TABLE_DUMP     = "\x13";
	const COMMAND_CONNECT_OUT    = "\x14";

	const DEFAULT_PORT_NUMBER = 3306;
	const BUFFER_LENGTH       = 1460;
	const DEFAULT_UNIX_SOCKET = '/tmp/mysql.sock';

	public $hostname   = NULL;
	public $unixsocket = self::DEFAULT_UNIX_SOCKET;
	public $port       = self::DEFAULT_PORT_NUMBER;
	public $database   = NULL;
	public $user       = NULL;
	public $password   = NULL;
	public $timeout    = 60;
	public $socket     = NULL;
	public $salt                 = '';
	public $protocol_version     = NULL;
	public $client_capabilities  = 0;
	public $affected_rows_length = 0;

	// state
	private $loop = NULL;
	private $conn = NULL;
	private $state = self::STATE_CONNECT;
	private $debug = FALSE;

	function __construct($args,$loop)
	{
		$keys = array(
			'hostname'
			,'unixsocket'
			,'port'
			,'database'
			,'user'
			,'password'
			,'timeout'
			,'debug'
		);
		foreach($keys as $key)
			if ( array_key_exists($key,$args) )
				$this->$key = $args[$key];

		$this->loop = $loop;

		$this->_connect();
		//$this->_get_server_information();
		//$this->_request_authentication();

		return;
	}

	public function query($sql)
	{
		return $this->_execute_command(COMMAND_QUERY, $sql);
	}

	public function create_database($db_name)
	{
		return $this->_execute_command(COMMAND_CREATE_DB, $db_name);
	}


	public function drop_database($db_name)
	{
		return $this->_execute_command(COMMAND_DROP_DB, $db_name);
	}

	public function close()
	{
		if ( ! $this->can('send') ) return;

		$quit_message =
			chr(strlen(COMMAND_QUIT)). "\x00\x00\x00". COMMAND_QUIT;
		$this->socket->send($quit_message, 0);
		$this->_dump_packet($quit_message);
		$this->socket->close();
	}


	public function get_affected_rows_length()
	{
		$this->affected_rows_length;
	}


	public function get_insert_id()
	{
		$this->insert_id;
	}

	public function create_record_iterator()
	{
		if ( ! $this->has_selected_record )
			return NULL;

		$record = new \Net\Mysql\RecordIterator($this->selected_record);
		$this->selected_record = NULL;
		$record->parse();
		return $record;
	}

	public function has_selected_record()
	{
		return $this->selected_record ? 1 : NULL;
	}

	public function is_error()
	{
		return $this->error_code ? 1 : NULL;
	}

	public function get_error_code()
	{
		return $this->error_code;
	}

	public function get_error_message()
	{
		return $this->server_message;
	}

	private function _connect()
	{
		echo "Connecting...".PHP_EOL;

		if ( $this->hostname )
		{
			$socket_addr = $this->hostname .':'. $this->port;

			if ( $this->debug )
				echo "Connecting to: tcp://{$socket_addr}\n";

			$this->socket = new \React\Socket\Connector($this->loop);

			$fx_connect = array($this,'onConnect');
			$fx_conn_err = array($this,'onConnectError');

			//Debug::d_debug(__FILE__,__FUNCTION__,__LINE__,"Connect to: {$socket_addr}.");
			$this->socket
				->connect($socket_addr)
				->then( $fx_connect, $fx_conn_err);
		}
		/*else {
			printf "Use UNIX Socket: %s\n", $this->unixsocket if $this->debug;
			$mysql = IO::Socket::UNIX->new(
				Type => SOCK_STREAM,
				Peer => $this->unixsocket,
			) or croak "Couldn't connect to $this->unixsocket: $@";
		}
		$mysql->autoflush(1);*/
	}

	public function onConnect (\React\Socket\ConnectionInterface $conn)
	{
		echo "onConnect".PHP_EOL;
		//Debug::fx_in(__FILE__,__FUNCTION__,__LINE__,$conn);
		$this->conn = $conn;

		// reset reconnect delay
		//$this->reconnectSeconds = 1; // default

		$conn->on('data',  array($this, 'onData') );
		$conn->on('close', array($this, 'onClose') );
		
		return;
	}

	public function onConnectError ($err)
	{
		//Debug::fx_in(__FILE__,__FUNCTION__,__LINE__,$err);

		//Debug::d_error(__FILE__,__FUNCTION__,__LINE__,"Failed to connect to hub.");

		// backoff reconnect
		/*if ( $this->reconnectSeconds == 1 )
			$this->reconnectSeconds = 5;
		elseif ( $this->reconnectSeconds == 5 )
			$this->reconnectSeconds = 30;
		elseif ( $this->reconnectSeconds == 30 )
			$this->reconnectSeconds = 60;*/

		// XXX: create reconnect timer function
		//Debug::d_debug(__FILE__,__FUNCTION__,__LINE__,'adding reconnect timer: '. $this->reconnectSeconds .'s');
		//$this->timer = $this->loop->addTimer($this->reconnectSeconds, array($this, 'connect') );

		die('unable to connect'.PHP_EOL);
		exit;

		Debug::fx_out(NULL,__FILE__,__FUNCTION__,__LINE__);
	}

	public function onData ($packet)
	{
		//Debug::fx_in(__FILE__,__FUNCTION__,__LINE__,$data);

		echo "onData".PHP_EOL;

		if ( $this->state == self::STATE_CONNECT )
		{
			$this->_get_server_information($packet);
			$this->_send_login_message();
			return;
		}

		if ( $this->state == self::STATE_AUTH )
		{
			$this->_handle_authentication($packet);
			return;
		}

		if ( $this->state == self::STATE_PENDING_RESULT )
		{
			$this->_dump_packet($packet);
			$this->_reset_status;

			if ( $this->_is_error($packet) )
				return $this->_set_error_by_packet($packet);

			elseif ( $this->_is_select_query_result($packet) )
				return $this->_get_record_by_server($packet);
	
			elseif ( $this->_is_update_query_result($packet) )
				return $this->_get_affected_rows_information_by_packet($packet);
	
			else
				die('Unknown Result: '. $this->_get_result_length($packet). 'byte');
		}

	}
	
	public function write($data)
	{
		echo "write packet:".PHP_EOL;
		$this->conn->write($data);
		$this->_dump_packet($data);
	}

	public function state($val=NULL)
	{
		if ( $val === NULL )
		{
			echo "state: ". $this->state .PHP_EOL;
			return $this->state;
		}
		
		echo "state <-- ". $val .PHP_EOL;
		$this->state = $val;
		$this->emit($val,[$this]);
		
		return;
	}

	private function _get_server_information($message)
	{
		$this->_dump_packet($message);

		$i = 0;
		$packet_length = ord(substr($message, $i, 1));

		$i += 4;
		$this->protocol_version = ord(substr($message, $i, 1));
		if ($this->debug)
			echo "Protocol Version: {$this->protocol_version}\n";
		if ($this->protocol_version == 10)
			$this->client_capabilities = 1;

		$i++;
		$string_end = strpos($message, "\0", $i) - $i;
		$this->server_version = substr($message, $i, $string_end);
		if ($this->debug)
			echo "Server Version: {$this->server_version}\n";

		$i += $string_end + 1;
		$this->server_thread_id = unpack('v', substr($message, $i, 2));

		$i += 4;
		$this->salt = substr($message, $i, 8);

		$i += 8+1;
		if (strlen($message) >= $i + 1)
			$i += 1;

		if (strlen($message) >= $i + 18)
		{
			# get server_language
			# get server_status
		}

		$i += 18 - 1;
		if (strlen($message) >= $i + 12 - 1) {
			$this->salt .= substr($message, $i, 12);
		}
		if ($this->debug)
			echo "Salt: {$this->salt}\n";

	}

	private function _handle_authentication($packet)
	{
		echo "_handle_authentication: start". PHP_EOL;
		$this->_dump_packet($packet);
		if ( $this->_is_error($packet) )
		{
			$this->conn->close();
			if (strlen($packet) < 7)
				die('Authentication timeout'.PHP_EOL);
			die(substr($packet, 7).PHP_EOL);
		}
		if ( $this->debug )
			echo "connect database\n";
			
		$this->state(self::STATE_READY);
	}

	private function _send_login_message()
	{
		echo "_send_login_message".PHP_EOL;
		$pw = \Net\Mysql\Password::scramble($this->password, $this->salt, $this->client_capabilities);
		$body = "\0\0\x01\x0d\xa6\03\0\0\0\0\x01";
		$body .= "\x21\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
		$body .= $this->user ."\0";
		$body .= "\x14".$pw;
		$body .= $this->database ."\0";
		
		$login_message = chr(strlen($body)-3). $body;
		$this->write($login_message);
		//$this->_dump_packet($login_message);

		$this->state(self::STATE_AUTH);
	}

	private function _execute_command($command,$sql)
	{
		$message = pack('V', strlen($sql) + 1). $command. $sql;
		$this->write($message);
		$this->_dump_packet($message);
		$this->state(self::STATE_PENDING_RESULT);
	}

	private function _set_error_by_packet($packet)
	{
		$error_message = $this->_get_server_message($packet);
		$this->server_message = $error_message;
		$this->error_code     = $this->_get_error_code($packet);
		return NULL;
	}

	private function _get_record_by_server($packet)
	{
		$this->_get_column_length($packet);
		while ($this->_has_next_packet($packet)) {
			$next_result;
			$this->socket->recv($next_result, BUFFER_LENGTH, 0);
			$packet .= $next_result;
			$this->_dump_packet($next_result);
		}
		return $this->selected_record = $packet;
	}

	private function _get_affected_rows_information_by_packet($packet)
	{
		$this->affected_rows_length = $this->_get_affected_rows_length($packet);
		$this->insert_id = $this->_get_insert_id($packet);
		$this->server_message = $this->_get_server_message($packet);
		return $this->affected_rows_length;
	}

	private function _is_error($packet)
	{
		echo "_is_error" .PHP_EOL;
		if ( strlen($packet) < 4 )
		{
			echo "_is_error: packet size less than 4, return true" .PHP_EOL;
			return true;
		}
		$test = ord(substr($packet, 4)) == 255;
		echo "_is_error: return: ". json_encode($test) .PHP_EOL;
		return $test;
	}

	private function _is_select_query_result($packet)
	{
		if ($this->_is_error($packet))
			return NULL;
		return ord(substr($packet, 4)) >= 1;
	}

	private function _is_update_query_result($packet)
	{
		if ($this->_is_error($packet))
			return NULL;
		return ord(substr($packet, 4)) == 0;
	}

	private function _get_result_length($packet)
	{
		return ord(substr($packet, 0, 1));
	}

	private function _get_column_length($packet)
	{
		return ord(substr($packet, 4));
	}

	private function _get_affected_rows_length($packet)
	{
		$pos = 5;
		return \Net\MySQL\Util::get_field_length($packet, $pos);
	}

	private function _get_insert_id($packet)
	{
		if ( ord(substr($packet, 6, 1) != 0xfc ) )
			return ord(substr($packet, 6, 1));
		return unpack('v', substr($packet, 7, 2));
	}

	private function _get_server_message($packet)
	{
		if (strlen($packet) < 7)
			return '';
		substr($packet, 7);
	}

	private function _get_error_code($packet)
	{
		$this->_is_error($packet)
			or die("_get_error_code(): Is not error packet");
		return unpack('v', substr($packet, 5, 2));
	}

	private function _reset_status()
	{
		$this->insert_id       = 0;
		$this->server_message  = '';
		$this->error_code      = NULL;
		$this->selected_record = NULL;
	}

	private function _has_next_packet($packet)
	{
		#substr($_[0], -1) ne "\xfe";
		#$this->_dump_packet(substr($_[0], -5));
		return substr($packet, -5, 1) != "\xfe";
	}

	private function _dump_packet($packet)
	{
		echo "_dump_packet: ".PHP_EOL;
		$lines = str_split($packet,16);
		foreach($lines as $line)
		{
			$hexline = '';
			$strline = '';
			$chars = str_split($line);
			foreach($chars as $char)
			{
				$n = ord($char);
				$hexline .= sprintf(' %02X',$n);
				if ( $n > 31 && $n < 127 )
					$strline .= $char;
				else $strline .= '.';
			}
			//$missing = 48 - strlen($hexline);
			//if ( $missing > 0 ) 
			echo str_pad($hexline,48,' ') .'  '. $strline .PHP_EOL;
		}
	}
}

namespace Net\Mysql;

/*
class RecordIterator
{

	const NULL_COLUMN           = 251;
	const UNSIGNED_CHAR_COLUMN  = 251;
	const UNSIGNED_SHORT_COLUMN = 252;
	const UNSIGNED_INT24_COLUMN = 253;
	const UNSIGNED_INT32_COLUMN = 254;
	const UNSIGNED_CHAR_LENGTH  = 1;
	const UNSIGNED_SHORT_LENGTH = 2;
	const UNSIGNED_INT24_LENGTH = 3;
	const UNSIGNED_INT32_LENGTH = 4;
	const UNSIGNED_INT32_PAD_LENGTH = 4;


	public function new()
	{
		$class = shift;
		$packet = shift;
		bless {
			packet   => $packet,
			position => 0,
			column   => [],
		}, $class;
	}


	public function parse()
	{
		$self = shift;
		$this->_get_column_length;
		$this->_get_column_name;
	}


	public function each()
	{
		$self = shift;
		@result;
		return NULL if $this->is_end_of_packet;

		for (1..$this->column_length) {
			push @result, $this->_get_string_and_seek_position;
		}
		$this->position += 4;

		return \@result;
	}


	public function is_end_of_packet()
	{
		$self = shift;
		return substr($this->packet, $this->position, 1) eq "\xFE";
	}


	public function get_field_length()
	{
		$self = shift;
		$this->column_length;
	}


	public function get_field_names()
	{
		$self = shift;
		map { $_->column } @{$this->column};
	}


	private function _get_column_length()
	{
		$self = shift;
		$this->position += 4;
		$this->column_length = ord substr $this->packet, $this->position, 1;
		$this->position += 5;
		printf "Column Length: %d\n", $this->column_length
			if Net::MySQL->debug;
	}


	private function _get_column_name()
	{
		$self = shift;

		for $i (1.. $this->column_length) {
			$this->_get_string_and_seek_position;
			$this->_get_string_and_seek_position;
			$table = $this->_get_string_and_seek_position;
			$this->_get_string_and_seek_position;
			$column = $this->_get_string_and_seek_position;
			$this->_get_string_and_seek_position;
			push @{$this->column}, {
				table  => $table,
				column => $column,
			};
			$this->_get_string_and_seek_position;
			$this->position += 4;
		}
		$this->position += 9;
		printf "Column name: '%s'\n",
			join ", ", map { $_->column } @{$this->column}
				if Net::MySQL->debug;
	}


	private function _get_string_and_seek_position()
	{
		$self = shift;

		$length = $this->_get_field_length();

		return NULL unless defined $length;

		$string = substr($this->packet, $this->position, $length);
		$this->position += $length;
		return $string;
	}


	private function _get_field_length()
	{
		$self = shift;
		return Net::MySQL::Util::get_field_length($this->packet, \$this->position);
	}

}


class Util
{

	const NULL_COLUMN           = 251;
	const UNSIGNED_CHAR_COLUMN  = 251;
	const UNSIGNED_SHORT_COLUMN = 252;
	const UNSIGNED_INT24_COLUMN = 253;
	const UNSIGNED_INT32_COLUMN = 254;
	const UNSIGNED_CHAR_LENGTH  = 1;
	const UNSIGNED_SHORT_LENGTH = 2;
	const UNSIGNED_INT24_LENGTH = 3;
	const UNSIGNED_INT32_LENGTH = 4;
	const UNSIGNED_INT32_PAD_LENGTH = 4;


	public function get_field_length()
	{
		$packet = shift;
		$pos = shift;

		$head = ord substr(
			$packet,
			$$pos,
			UNSIGNED_CHAR_LENGTH
		);
		$$pos += UNSIGNED_CHAR_LENGTH;

		return NULL if $head == NULL_COLUMN;
		if ($head < UNSIGNED_CHAR_COLUMN) {
			return $head;
		}
		elsif ($head == UNSIGNED_SHORT_COLUMN) {
			$length = unpack 'v', substr(
				$packet,
				$$pos,
				UNSIGNED_SHORT_LENGTH
			);
			$$pos += UNSIGNED_SHORT_LENGTH;
			return $length;
		}
		elsif ($head == UNSIGNED_INT24_COLUMN) {
			$int24 = substr(
				$packet, $$pos,
				UNSIGNED_INT24_LENGTH
			);
			$length = unpack('C', substr($int24, 0, 1))
			          + (unpack('C', substr($int24, 1, 1)) << 8)
				  + (unpack('C', substr($int24, 2, 1)) << 16);
			$$pos += UNSIGNED_INT24_LENGTH;
			return $length;
		}
		else {
			$int32 = substr(
				$packet, $$pos,
				UNSIGNED_INT32_LENGTH
			);
			$length = unpack('C', substr($int32, 0, 1))
			          + (unpack('C', substr($int32, 1, 1)) << 8)
				  + (unpack('C', substr($int32, 2, 1)) << 16)
				  + (unpack('C', substr($int32, 3, 1)) << 24);
			$$pos += UNSIGNED_INT32_LENGTH;
			$$pos += UNSIGNED_INT32_PAD_LENGTH;
			return $length;
		}
	}
}
*/

class Password
{

	public static function scramble($password,$hash_seed)
	{
		if ( !$password || !strlen($password) ) return '';
		return self::_make_scrambled_password($hash_seed, $password);
	}

	public static function _make_scrambled_password($message,$password)
	{
		if ( !in_array('sha1',hash_algos() ) )
			die('sha1 hash algorithm is required'.PHP_EOL);

		$stage1 = hash('sha1',$password,true);
		$stage2 = hash('sha1',$stage1,true);
		$result = hash('sha1',$message.$stage2,true);

		$scrambled_pw = self::_my_crypt($result, $stage1);
		return $scrambled_pw;
	}

	public static function _my_crypt($s1,$s2)
	{
		$l = strlen($s1) - 1;
		$result = '';
		for($i=0; $i <= $l; $i++)
		{
			$result .= pack('C', unpack('C', substr($s1, $i, 1)) ^ unpack('C', substr($s2, $i, 1)));
		}
		return $result;
	}

}

class Password32
{
	
	public static function scramble($password,$hash_seed,$client_capabilities)
	{
		if ( !$password || !strlen($password) ) return '';

		$hsl = strlen($hash_seed);
		$out = array();
		$hash_pass = self::_get_hash($password);
		$hash_mess = self::_get_hash($hash_seed);

		if ($client_capabilities < 1)
		{
			$max_value = 0x01FFFFFF;
			$seed = self::_xor_by_long($hash_pass[0], $hash_mess[0]) % $max_value;
			$seed2 = int($seed / 2);
		} else {
			$max_value= 0x3FFFFFFF;
			$seed  = self::_xor_by_long($hash_pass[0], $hash_mess[0]) % $max_value;
			$seed2 = self::_xor_by_long($hash_pass[1], $hash_mess[1]) % $max_value;
		}
		$dMax = $max_value;

		for ($i=0; $i < $hsl; $i++)
		{
			$seed  = int(($seed * 3 + $seed2) % $max_value);
			$seed2 = int(($seed + $seed2 + 33) % $max_value);
			$dSeed = $seed;
			$dRes = $dSeed / $dMax;
			$out[] = int($dRes * 31) + 64;
		}

		if ($client_capabilities == 1)
		{
			# Make it harder to break
			$seed  = ($seed * 3 + $seed2  ) % $max_value;
			$seed2 = ($seed + $seed2 + 33 ) % $max_value;
			$dSeed = $seed;

			$dRes = $dSeed / $dMax;
			$e = int($dRes * 31);
			for ($i=0; $i < $hsl ; $i++)
			{
				$out[$i] ^= $e;
			}
		}
		$xform = function($a) { return chr($a); };
		return implode('', array_map($xform,$out));
	}

	private static function _get_hash($password)
	{
		$nr = 1345345333;
		$add = 7; 
		$nr2 = 0x12345671;
		$pwlen = strlen($password);

		for ($i=0; $i < $pwlen; $i++)
		{
			$c = substr($password, $i, 1);
			if ( $c == ' ' || $c == "\t" ) continue;
			$tmp = ord($c);
			$value = ( ( self::_and_by_char($nr, 63) + $add ) * $tmp ) + $nr * 256;
			$nr = self::_xor_by_long($nr, $value);
			$nr2 += self::_xor_by_long(($nr2 * 256), $nr);
			$add += $tmp;
		}
		$result = array( self::_and_by_long($nr, 0x7fffffff), self::_and_by_long($nr2, 0x7fffffff) );
		return $result;
	}


	private static function _and_by_char($source,$mask)
	{
		return $source & $mask;
	}


	private static function _and_by_long($source,$mask=0xFFFFFFFF)
	{
		return _cut_off_to_long($source) & _cut_off_to_long($mask);
	}


	private static function _xor_by_long($source,$mask=0)
	{
		return _cut_off_to_long($source) ^ _cut_off_to_long($mask);
	}


	private static function _cut_off_to_long($source)
	{
		if ( $source > 0xFFFFFFF )
			$source = $source % (0xFFFFFFFF + 1);
		return $source;
	}

}

?>