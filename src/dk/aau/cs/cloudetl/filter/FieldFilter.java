/*
 *
 * Copyright (c) 2011, Xiufeng Liu (xiliu@cs.aau.dk) and the eGovMon Consortium
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *
 */
package dk.aau.cs.cloudetl.filter;

import java.io.Serializable;
import java.util.ArrayList;

import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.filter.rule.Rule;
import dk.aau.cs.cloudetl.io.RecordWritable;

public class FieldFilter implements Serializable  {
    
    private final String name;
    private final ArrayList<Rule> rules = new ArrayList<Rule>();
    private Rule currentRule;
    private Field currentField;
    
    public FieldFilter(String name) {
        this.name = name;
    }
        
    public Field getCurrentField() {
        return currentField;
    }
    
    public String getName() {
        return name;
    }
    
    public FieldFilter addRule(Rule ... rules) {
        for (int i = 0; i < rules.length; i++) {
            this.rules.add(rules[i]);
        }
        return this;
    }

    
    public boolean allow(RecordWritable record) {
    	currentField = record.getField(name);
        for (int i = 0; i < rules.size(); i++) {
            currentRule = rules.get(i);
            if (!currentRule.allow(currentField)) {
                return false;
            }
        }
        return true;
    }
    
    public String toString() {
        StringBuffer s = new StringBuffer();
        return s.toString();
    }
}
