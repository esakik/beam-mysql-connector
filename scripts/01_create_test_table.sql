DROP TABLE IF EXISTS `tests`;

CREATE TABLE IF NOT EXISTS `tests`
(
  `id`               INT(20) AUTO_INCREMENT,
  `name`             VARCHAR(20) NOT NULL,
  PRIMARY KEY (`id`)
) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;