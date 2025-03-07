package com.bfr.vergiftungsregisterbackend.gizadaptererfurt.proecess;

import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.write.FallNormalisiert;
import com.bfr.vergiftungsregisterbackend.gizadaptererfurt.read.FallErfurt;
import org.springframework.batch.item.ItemProcessor;

public class FallErfurtProcessor implements ItemProcessor<FallErfurt, FallNormalisiert> {

    @Override
    public FallNormalisiert process(FallErfurt item) throws Exception {
        FallNormalisiert fallNormalisiert = new FallNormalisiert(item.getFanrze(), item.getMj(), item.getOrtbez(), item.getAfnam(), item.getSubnam1());
        System.out.println(fallNormalisiert);
        return fallNormalisiert;
    }
}
