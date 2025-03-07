package com.bfr.vergiftungsregisterbackend.gizadaptererfurt.write;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;

import java.util.Collections;


// Utility class to encapsulate the logic of manipulating schema and records of the source database
public class DatabaseUtils {

    public static String constructUpdateFallNormalisiertQuery(String tableName) {
        return "insert into " + tableName + " (fall_id, meldejahr, ortsbezeichnung, melde_id, substanz) values (?, ?, ?, ?, ?) ";
    }

    public static ItemPreparedStatementSetter<FallNormalisiert> UPDATE_FALL_DATEN_NORMALISIERT_SETTER = (item, ps) -> {
        ps.setString(1, item.getFallId());
        ps.setString(2, item.getMeldejahr());
        ps.setString(3, item.getOrtbezeichnung());
        ps.setString(4, item.getMelderId());
        ps.setString(5, item.getSubstanz());
    };

    // Insert session action record into the specified table

    // Query provider to select all records from session actions table with the specified name
    public static PostgresPagingQueryProvider selectAllSessionActionsProvider(String tableName) {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("id, user_id, action_type, amount");
        queryProvider.setFromClause(tableName);
        queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));
        return queryProvider;
    }
}
