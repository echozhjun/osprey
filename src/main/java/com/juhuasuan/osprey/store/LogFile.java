/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.juhuasuan.osprey.store;

import java.io.File;
import java.io.IOException;

class LogFile extends DataFile {
    LogFile(File file, int n) throws IOException {
        this(file, n, false);
    }

    LogFile(File file, int n, boolean force) throws IOException {
        super(file, n, force);
        long count = fc.size() / OpItem.LENGTH;
        if (count * OpItem.LENGTH < fc.size()) {
            fc.truncate(count * OpItem.LENGTH);
            fc.position(count * OpItem.LENGTH);
        }
    }

}
