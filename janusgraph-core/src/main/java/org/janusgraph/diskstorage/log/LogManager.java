// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.log;

import org.janusgraph.diskstorage.BackendException;

/**
 * Manager interface for opening {@link Log}s against a particular Log implementation.
 * 管理器界面，用于针对特定的Log实现打开{@link Log}。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogManager {

    /**
     * Opens a log for the given name.
     * 打开给定名称的日志。
     * <p>
     * If a log with the given name already exists, the existing log is returned.
     * 如果已经存在具有给定名称的日志，则返回现有日志。
     *
     * @param name Name of the log to be opened 要打开的日志名称
     * @return
     * @throws org.janusgraph.diskstorage.BackendException
     */
    Log openLog(String name) throws BackendException;

    /**
     * Closes the log manager and all open logs (if they haven't already been explicitly closed)
     * 关闭日志管理器和所有打开的日志（如果尚未明确关闭它们）
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void close() throws BackendException;

}
