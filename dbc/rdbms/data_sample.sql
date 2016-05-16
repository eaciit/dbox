-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Jan 26, 2016 at 05:37 AM
-- Server version: 5.6.20
-- PHP Version: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `test`
--

DELIMITER $$
--
-- Procedures
--
CREATE DEFINER=`root`@`localhost` PROCEDURE `getUmur`(
IN nama VARCHAR(255), 
OUT age INT)
BEGIN
SELECT umur 
INTO age 
FROM tes 
where name = nama;
END$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `oke`(IN con VARCHAR(255))
BEGIN
  SELECT id,name FROM tes
  WHERE name = con;
END$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `selectIn`(IN `val1` INT, IN `val2` INT, OUT `result` VARCHAR(25))
BEGIN
 SELECT name
 INTO result 
 FROM tes
 where umur in (val1, val2) limit 1;
END$$

DELIMITER ;

DELIMITER $$
CREATE PROCEDURE insertdata(IN idIn varchar(255), IN nameIn varchar(255), IN umurIn INT)
BEGIN
 INSERT INTO tes(id, name, tanggal, umur) 
 VALUES (idIn,nameIn,now(),umurIn);
END$$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE `getUmurAjah`(IN nama VARCHAR(255))
BEGIN
SELECT umur
FROM tes 
where name = nama;
END$$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE updatedata(IN idIn varchar(255), IN idCondIn varchar(255), IN nameIn varchar(255), IN umurIn INT) 
BEGIN 
 UPDATE tes 
 SET 
 id = idIn, 
 name = nameIn, 
 tanggal = now(), 
 umur = umurIn 
 WHERE id = idCondIn; 
END $$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE updatedatademo(IN idCondIn varchar(255), IN amountIn INT, IN namaIn varchar(255)) 
BEGIN 
 UPDATE orders 
 SET
 nama = namaIn, 
 amount = amountIn
 WHERE id = idCondIn; 
END $$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE deletedata(IN idCondIn varchar(255)) 
BEGIN 
 DELETE 
 FROM tes
 WHERE id = idCondIn; 
END $$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE twooutput(out umurOut int, IN nameIn varchar(255), IN umurIn int, out nameOut varchar(255))
BEGIN
 select name, umur
 into nameOut, umurOut
 from tes
 where name = nameIn and umur = umurIn;
END$$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE inoutproc(inout umur int, IN nameIn varchar(255), out nameOut varchar(255))
BEGIN
 select name, umur
 into nameOut, umur
 from tes
 where name = nameIn and umur = umur;
END$$
DELIMITER ;
-- --------------------------------------------------------

-- ========================================== SQL SERVER ==================================

GO
CREATE PROCEDURE getUmur 
    @nama nvarchar(50)
AS 
    SELECT nama, umur
    FROM tes
    WHERE nama = @nama;
GO

GO
CREATE PROCEDURE getUmurIn 
    @umur1 int,
    @umur2 int
AS 
    SELECT nama, umur
    FROM tes
    WHERE umur IN (@umur1, @umur2);
GO

CREATE PROCEDURE staticproc
AS 
    SELECT nama, umur
    FROM tes;
GO

GO

-- INSERT ========================================

CREATE PROCEDURE insertdata 
	@idIn nvarchar(50),
    @namaIn nvarchar(255),
    @umurIn int
AS 
    INSERT INTO tes(player_id, nama, tanggal, umur) 
 	VALUES (@idIn, @namaIn, getdate(), @umurIn);
GO

-- UPDATE ========================================

GO
CREATE PROCEDURE updatedata 
	@idIn nvarchar(50),
    @idCondIn nvarchar(50),
    @namaIn nvarchar(255),
    @umurIn int
AS 
	UPDATE tes 
	SET 
	 player_id = @idIn, 
	 nama = @namaIn, 
	 tanggal = getdate(), 
	 umur = @umurIn
	WHERE player_id = @idCondIn; 
GO

-- DELETE ========================================

GO
CREATE PROCEDURE deletedata 
    @idCondIn nvarchar(50)
AS 
	DELETE 
 	FROM tes
 	WHERE player_id = @idCondIn; 
GO

-- ========================================== ORACLE ==================================
create or replace PROCEDURE getUmur (
  p_nama in tes.nama%type,
  o_umur out tes.umur%type)
AS
BEGIN
  SELECT umur 
  into o_umur
  FROM tes
  WHERE nama = p_nama;
END;
-- ==========================================
var o_umur number;
EXECUTE getUmur ('Vidal', :o_umur);
print :o_umur;
-- ==========================================
create or replace PROCEDURE getUmurIn (
  p_umur1 in tes.umur%type,
  p_umur2 in tes.umur%type,
  o_nama out tes.nama%type,
  o_umur out tes.umur%type)
AS
BEGIN
  SELECT nama, umur
  into o_nama, o_umur
  FROM tes
  WHERE umur IN (p_umur1, p_umur2);
END;
-- ==========================================
var o_nama varchar2(255);
var o_umur number;
EXECUTE getUmurIn ('20', '20', :o_nama, :o_umur);
print :o_nama;
print :o_umur;
-- ==========================================
create or replace PROCEDURE staticproc
AS
BEGIN
  delete from tes where player_id = 'ply032';
END;

-- INSERT ========================================

create or replace PROCEDURE insertdata (
  idIn in TES.PLAYER_ID%type,
  namaIn in TES.NAMA%type,
  umurIn in TES.UMUR%type)
AS
BEGIN
  INSERT INTO tes(player_id, nama, tanggal, umur) 
  VALUES (idIn, namaIn, current_date, umurIn);
END;
-- ==========================================
EXECUTE insertdata ('30', 'Costacurta', 40);
-- ==========================================

-- UPDATE ========================================

create or replace PROCEDURE updatedata (
  idIn in TES.PLAYER_ID%type,
  idCondIn in TES.PLAYER_ID%type,
  namaIn in TES.NAMA%type,
  umurIn in TES.UMUR%type)
AS
BEGIN
 UPDATE tes 
	SET 
	 player_id =idIn, 
	 nama = namaIn, 
	 tanggal = current_date, 
	 umur = umurIn
	WHERE player_id = idCondIn;
END;
-- ==========================================
EXECUTE updatedata ('ply030', '30', 'Payet', 25);
-- ==========================================

-- DELETE ========================================

create or replace PROCEDURE deletedata (
  idCondIn in TES.PLAYER_ID%type)
AS
BEGIN
  DELETE 
 	FROM tes
 	WHERE player_id = idCondIn;
END;

-- ==========================================
EXECUTE deletedata ('ply030');
-- ==========================================

-- ========================================== END OF STORED PROCEDURE ==================================

CREATE VIEW ItemOrder AS
SELECT nama, amount
FROM orders

--
-- Table structure for table `coba`
--

CREATE TABLE IF NOT EXISTS `coba` (
  `id` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `coba`
--

INSERT INTO `coba` (`id`, `name`) VALUES
('3', 'Bourne'),
('1', 'Abdul Ja''far'),
('2', 'Megaloman');

-- --------------------------------------------------------

--
-- Table structure for table `noid`
--

CREATE TABLE IF NOT EXISTS `noid` (
  `aidi` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ========================================== SQL SERVER ==================================

CREATE TABLE noid (
  aidi varchar(255) NOT NULL,
  nama varchar(255) NOT NULL
);

--
-- Dumping data for table `noid`
--

INSERT INTO `noid` (`aidi`, `name`) VALUES
('30', 'no multi, with data contains no ID'),
('30', 'no multi, with data contains no ID'),
('30', 'no multi, with data contains no ID');

-- ========================================== SQL SERVER ==================================

INSERT INTO noid (aidi, nama) VALUES
('30', 'no multi, with data contains no ID'),
('30', 'no multi, with data contains no ID'),
('30', 'no multi, with data contains no ID');



--
-- Table structure for table `orders`
--

CREATE TABLE IF NOT EXISTS `orders` (
  `id` varchar(255) NOT NULL,
  `nama` varchar(255) NOT NULL,
  `quantity` int(11) NOT NULL,
  `price` int(11) NOT NULL,
  `amount` int(11) NOT NULL,
  `status` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ========================================== SQL SERVER ==================================

CREATE TABLE orders (
  id varchar(255) NOT NULL,
  nama varchar(255) NOT NULL,
  quantity int NOT NULL,
  price int NOT NULL,
  amount int NOT NULL,
  status_item varchar(255) NOT NULL
);

--
-- Dumping data for table `orders`
--

INSERT INTO `orders` (`id`, `nama`, `quantity`, `price`, `amount`, `status`) VALUES
('ord001', 'buku', 3, 50000, 150000, 'available'),
('ord002', 'buku', 4, 50000, 200000, 'available'),
('ord003', 'buku', 2, 50000, 100000, 'available'),
('ord004', 'tas', 2, 200000, 400000, 'available'),
('ord005', 'tas', 4, 250000, 1000000, 'available'),
('ord006', 'dompet', 3, 100000, 300000, 'available'),
('ord007', 'dompet', 2, 125000, 250000, 'available'),
('ord008', 'stempel', 2, 50000, 100000, 'out of stock');

-- ========================================== SQL SERVER ==================================

INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES
('ord001', 'buku', 3, 50000, 150000, 'available'),
('ord002', 'buku', 4, 50000, 200000, 'available'),
('ord003', 'buku', 2, 50000, 100000, 'available'),
('ord004', 'tas', 2, 200000, 400000, 'available'),
('ord005', 'tas', 4, 250000, 1000000, 'available'),
('ord006', 'dompet', 3, 100000, 300000, 'available'),
('ord007', 'dompet', 2, 125000, 250000, 'available'),
('ord008', 'stempel', 2, 50000, 100000, 'out of stock');

-- ========================================== ORACLE ==================================
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord001', 'buku', 3, 50000, 150000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord002', 'buku', 4, 50000, 200000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord003', 'buku', 2, 50000, 100000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord004', 'tas', 2, 200000, 400000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord005', 'tas', 4, 250000, 1000000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord006', 'dompet', 3, 100000, 300000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord007', 'dompet', 2, 125000, 250000, 'available');
INSERT INTO orders (id, nama, quantity, price, amount, status_item) VALUES ('ord008', 'stempel', 2, 50000, 100000, 'out of stock');

-- --------------------------------------------------------

--
-- Table structure for table `tes`
--

CREATE TABLE IF NOT EXISTS `tes` (
  `id` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `tanggal` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `umur` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ========================================== SQL SERVER ==================================

CREATE TABLE tes (
  player_id varchar(255) NOT NULL,
  nama varchar(255) NOT NULL,
  tanggal timestamp NOT NULL,
  umur int NOT NULL
);

--
-- Dumping data for table `tes`
--

INSERT INTO `tes` (`id`, `name`, `tanggal`, `umur`) VALUES
('ply001', 'Bourne', '2016-01-26 03:26:39', 23),
('ply004', 'Kane', '2016-01-26 03:26:40', 29),
('ply005', 'Roy', '2016-01-26 03:26:40', 34),
('ply002', 'clyne', '2016-01-26 03:26:40', 25),
('ply007', 'Oscar', '2016-01-26 03:26:40', 24),
('ply008', 'Arnautovic', '2016-01-26 03:26:40', 21),
('ply009', 'Barkley', '2016-01-26 03:26:40', 20),
('ply010', 'Vidal', '2016-01-26 03:26:40', 21),
('ply011', 'Ramsey', '2016-01-26 03:26:40', 22),
('ply012', 'Agger', '2016-01-26 03:26:40', 23),
('ply013', 'Wijnaldum', '2016-01-26 03:26:40', 24),
('ply014', 'Ighalo', '2016-01-26 03:26:41', 25),
('ply015', 'Mahrez', '2016-01-26 03:26:41', 26),
('ply016', 'Toure', '2016-01-26 03:26:41', 26),
('ply017', 'Ivanovic', '2016-01-26 03:26:41', 27),
('ply018', 'Costa', '2016-01-26 03:26:41', 28),
('ply019', 'Chamberlain', '2016-01-26 03:26:41', 29),
('ply020', 'Hart', '2016-01-26 03:26:41', 30),
('ply021', 'Bruyne', '2016-01-26 03:26:41', 31),
('ply022', 'Aguero', '2016-01-26 03:26:41', 32);

-- ========================================== SQL SERVER ==================================

INSERT INTO tes (player_id, nama, tanggal, umur) VALUES
('ply001', 'Bourne', '2016-01-26 03:26:39', 23),
('ply004', 'Kane', '2016-01-26 03:26:40', 29),
('ply005', 'Roy', '2016-01-26 03:26:40', 34),
('ply002', 'clyne', '2016-01-26 03:26:40', 25),
('ply007', 'Oscar', '2016-01-26 03:26:40', 24),
('ply008', 'Arnautovic', '2016-01-26 03:26:40', 21),
('ply009', 'Barkley', '2016-01-26 03:26:40', 20),
('ply010', 'Vidal', '2016-01-26 03:26:40', 21),
('ply011', 'Ramsey', '2016-01-26 03:26:40', 22),
('ply012', 'Agger', '2016-01-26 03:26:40', 23),
('ply013', 'Wijnaldum', '2016-01-26 03:26:40', 24),
('ply014', 'Ighalo', '2016-01-26 03:26:41', 25),
('ply015', 'Mahrez', '2016-01-26 03:26:41', 26),
('ply016', 'Toure', '2016-01-26 03:26:41', 26),
('ply017', 'Ivanovic', '2016-01-26 03:26:41', 27),
('ply018', 'Costa', '2016-01-26 03:26:41', 28),
('ply019', 'Chamberlain', '2016-01-26 03:26:41', 29),
('ply020', 'Hart', '2016-01-26 03:26:41', 30),
('ply021', 'Bruyne', '2016-01-26 03:26:41', 31),
('ply022', 'Aguero', '2016-01-26 03:26:41', 32);

-- ========================================== ORACLE ==================================

INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply001', 'Bourne', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 23);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply00r', 'Kane', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 29);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply005', 'Roy', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 34);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply002', 'clyne', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 25);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply007', 'Oscar', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 24);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply008', 'Arnautovic', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 21);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply009', 'Barkley', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 20);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply010', 'Vidal', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 21);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply011', 'Ramsey', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 22);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply012', 'Agger', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 23);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply013', 'Wijnaldum', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 24);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply014', 'Ighalo', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 25);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply015', 'Mahrez', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 26);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply016', 'Toure', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 26);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply017', 'Ivanovic', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 27);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply018', 'Costa', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 28);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply019', 'Chamberlain', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 29);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply020', 'Hart', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 30);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply021', 'Bruyne', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 31);
INSERT INTO tes (player_id, nama, tanggal, umur) VALUES ('ply022', 'Aguero', to_date('2016-01-26 03:26:39','yyyy-mm-dd hh24:mi:ss'), 32);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;


SELECT player_id,nama,tanggal,umur 
FROM tes 
WHERE umur > '25' 
ORDER BY umur ASC, nama DESC
OFFSET 5 ROWS
FETCH NEXT 5 ROWS ONLY;

SELECT player_id,nama,tanggal,umur 
FROM tes 
WHERE umur > '25' 
ORDER BY umur ASC, nama DESC
OFFSET 5 ROWS;

SELECT TOP 5 player_id,nama,tanggal,umur 
FROM tes 
WHERE umur > '25' 
ORDER BY umur ASC, nama DESC

CREATE VIEW NamaUmur AS
SELECT name, umur
FROM tes

CREATE VIEW ItemOrder AS
SELECT nama, amount
FROM orders


SHOW FULL TABLES IN test WHERE TABLE_TYPE LIKE 'VIEW'

--
-- Table structure for table `tipedata`
--
-- =========================== SQL SERVER =========================
CREATE TABLE IF NOT EXISTS tipedata (
  t_int int NOT NULL,
  t_float float NOT NULL,
  t_bool bit NOT NULL,
  t_string varchar(255) NOT NULL,
  t_date datetime NOT NULL
);

-- =========================== POSTGRES =========================
CREATE TABLE IF NOT EXISTS tipedata (
  t_int integer NOT NULL,
  t_float float NOT NULL,
  t_bool boolean NOT NULL,
  t_string varchar(255) NOT NULL,
  t_date timestamp without time zone NOT NULL
);

--
-- Dumping data for table `tipedata`
--
-- =========================== SQL SERVER =========================
INSERT INTO tipedata (t_int, t_float, t_bool, t_string, t_date) VALUES
(1, 32.25, 1, 'string true', '2016-03-14 18:45:10'),
(14, 23.45, 0, 'string false', '2016-03-14 18:45:52');

-- =========================== POSTGRES =========================
INSERT INTO tipedata (t_int, t_float, t_bool, t_string, t_date) VALUES
(1, 32.25, true, 'string true', '2016-03-14 18:45:10'),
(14, 23.45, false, 'string false', '2016-03-14 18:45:52');
