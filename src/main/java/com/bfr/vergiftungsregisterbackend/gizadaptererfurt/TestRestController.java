package com.bfr.vergiftungsregisterbackend.gizadaptererfurt;

import org.springframework.batch.core.*;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;


@RestController
public class TestRestController {

    @Autowired
    @Qualifier("asyncJobLauncher")
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("fallErfurtJob")
    private AbstractJob fallErfurtJob;


    @PostMapping("/start-fallverarbeitung")
    public String startFallverarbeitung() throws Exception {
        jobLauncher.run(fallErfurtJob, buildUniqueJobParameters());
        return "Successfully started!\n";
    }


    // Building unique job parameters to not care about restarts
    private static JobParameters buildUniqueJobParameters() {
        return new JobParametersBuilder()
                .addString(UUID.randomUUID().toString(), UUID.randomUUID().toString())
                .toJobParameters();
    }
}
