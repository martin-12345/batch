/*
 *   Copyright (c) 2023 Martin Newstead.  All Rights Reserved.
 *
 *   The author makes no representations or warranties about the suitability of the
 *   software, either express or implied, including but not limited to the
 *   implied warranties of merchantability, fitness for a particular
 *   purpose, or non-infringement. The author shall not be liable for any damages
 *   suffered by licensee as a result of using, modifying or distributing
 *   this software or its derivatives.
 */
package com.martin;

import org.springframework.batch.item.file.FlatFileItemReader;

public class HeaderValidatingFileItemReader<T> extends FlatFileItemReader<T> {
    private boolean headerError = false;

    @Override
    protected void doOpen() throws Exception {
        try {
            super.doOpen();
        } catch(PersonFileHeaderException e) {
            headerError = true;
        }
    }
    protected T doRead() throws Exception {
        if(headerError)
            return null;
        return super.doRead();
    }
}
