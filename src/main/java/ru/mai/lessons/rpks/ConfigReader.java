package ru.mai.lessons.rpks;

import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Map;

public interface ConfigReader {
    public Config loadConfig(); // метод читает конфигурацию из файла *.conf

}
