package com.bfr.vergiftungsregisterbackend.gizadaptererfurt.read;

import org.springframework.batch.item.file.LineMapper;

public class FallErfurtCsvMapper implements LineMapper<FallErfurt> {

    @Override
    public FallErfurt mapLine(String line, int lineNumber) throws Exception {
       String[] fallErfurtFelder = line.split(";");
       FallErfurt fallErfurt = mapToFallErfurt(fallErfurtFelder);
       System.out.println(fallErfurt);
       return fallErfurt;

    }
    private FallErfurt mapToFallErfurt(String[] fallErfurtFelder) {
        return new FallErfurt(fallErfurtFelder[0], fallErfurtFelder[1], fallErfurtFelder[2], fallErfurtFelder[3], fallErfurtFelder[5]);
    }
}
