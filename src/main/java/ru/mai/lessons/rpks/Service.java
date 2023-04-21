package ru.mai.lessons.rpks;

import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Map;

public interface Service {

    public void start(Config config); // стартует приложение.
}
