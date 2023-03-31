package ru.mai.lessons.rpks;

import lombok.extern.slf4j.Slf4j;
import org.jooq.codegen.GenerationTool;
import ru.mai.lessons.rpks.impl.ConfigReaderImpl;
import ru.mai.lessons.rpks.impl.ServiceFiltering;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class ServiceFilteringMain {
    public static void main(String[] args) throws Exception {
//        GenerationTool.generate(
//                Files.readString(
//                        Path.of("jooq-config.xml")
//                )
//        );

        log.info("Start service Filtering");
        ConfigReader configReader = new ConfigReaderImpl();
        Service service = new ServiceFiltering(); // ваша реализация service
        service.start(configReader.loadConfig());
        log.info("Terminate service Filtering");
    }
}