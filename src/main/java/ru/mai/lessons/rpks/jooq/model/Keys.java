/*
 * This file is generated by jOOQ.
 */
package ru.mai.lessons.rpks.jooq.model;


import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;

import ru.mai.lessons.rpks.jooq.model.tables.FilterRules;
import ru.mai.lessons.rpks.jooq.model.tables.records.FilterRulesRecord;


/**
 * A class modelling foreign key relationships and constraints of tables in
 * public.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<FilterRulesRecord> FILTER_RULES_PKEY = Internal.createUniqueKey(FilterRules.FILTER_RULES, DSL.name("filter_rules_pkey"), new TableField[] { FilterRules.FILTER_RULES.ID }, true);
}
