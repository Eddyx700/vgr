package com.bfr.vergiftungsregisterbackend;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.test.ApplicationModuleTest;

@ApplicationModuleTest
class VergiftungsregisterbackendApplicationTests {

    @Test
    void contextLoads() {
        ApplicationModules.of(VergiftungsregisterbackendApplication.class).verify();
    }

}
