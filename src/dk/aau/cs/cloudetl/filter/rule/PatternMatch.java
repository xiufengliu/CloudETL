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
package dk.aau.cs.cloudetl.filter.rule;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dk.aau.cs.cloudetl.common.Field;

public class PatternMatch implements Rule, Serializable  {
    
 
	 private final ArrayList<Pattern> patterns = new ArrayList<Pattern>();

	    public PatternMatch() {
	    }

	    public PatternMatch(String ... regex) {
	        add(regex);
	    }

	    public PatternMatch(String regex, int flags) {
	        add(regex, flags);
	    }

	    public PatternMatch add(String ... regex) {
	        for (int i = 0; i < regex.length; i++) {
	            patterns.add(Pattern.compile(regex[i]));
	                }
	        return this;
	    }

	    public PatternMatch add(String regex, int flags) {
	        patterns.add(Pattern.compile(regex, flags));
	        return this;
	    }

	    public int getCount() {
	        return patterns.size();
	    }

	    public Pattern get(int index) {
	        return patterns.get(index);
	    }
	    
	    @Override
	    public boolean allow(Field field) {
	        if (field.isNull()) {
	            return false;
	        }
	        String string = field.getValueAsString();
	        for (int i = 0; i < getCount(); i++) {
	            Pattern pattern = get(i);
	            Matcher matcher = pattern.matcher(string);
	            if (matcher.matches()) {
	                return true;
	            }
	        }
	        return false;
	    }




}
