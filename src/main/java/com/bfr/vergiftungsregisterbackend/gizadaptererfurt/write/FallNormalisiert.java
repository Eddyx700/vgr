package com.bfr.vergiftungsregisterbackend.gizadaptererfurt.write;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class FallNormalisiert
{
    public static final String FALL_NORMALISIERT_TABLE_NAME = "fall_normalisiert";
    private final String fallId;
    private final String meldejahr;
    private final String ortbezeichnung;
    private final String melderId;
    private final String substanz;
}
