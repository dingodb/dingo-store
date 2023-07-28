// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGO_SERIAL_COUNTER_H_
#define DINGO_SERIAL_COUNTER_H_

#include <sys/time.h>
#include <iostream>
#include <stdint.h>
class Counter {
private:
    timeval s;
    timeval sc;

public:
    Counter() {
        gettimeofday(&s, 0);
    }
    void reStart() {
        gettimeofday(&s, 0);
    }
    void saveCounter() {
        gettimeofday(&sc, 0);
    }
    int timeElapsedBeforeLastSave() {
        return (((sc.tv_sec - s.tv_sec) * 1000000) + (sc.tv_usec - s.tv_usec));
    }

    int64_t timeElapsed() {
        timeval e;
        gettimeofday(&e, 0);
        return (((e.tv_sec - s.tv_sec) * 1000000) + (e.tv_usec - s.tv_usec));
    }

    int64_t mtimeElapsed() {
        timeval e;
        gettimeofday(&e, 0);
        return (((e.tv_sec - s.tv_sec) * 1000) + (e.tv_usec - s.tv_usec)/1000);
    }

    static std::string getSysTime() {
        time_t rawtime;
        char c[128];
        struct tm timeinfo;
        time(&rawtime);
        localtime_r(&rawtime, &timeinfo);
        asctime_r(&timeinfo, c);
        return std::string(c);
    }
    virtual ~Counter() {
    }
};

class Clock {
public:
    Clock() {
        m_total.tv_sec = 0;
        m_total.tv_nsec = 0;
        m_isStart = false;
    }

    void start() {
        if (!m_isStart) {
            clock_gettime(CLOCK_REALTIME, &m_start);
            m_isStart = true;
        }
    }

    void stop() {
        if (m_isStart) {
            struct timespec end;
            clock_gettime(CLOCK_REALTIME, &end);
            add(m_total, diff(m_start, end));
            m_isStart = false;
        }
    }

    time_t second() const {
        return m_total.tv_sec;
    }

    int64_t nsecond() const {
        return m_total.tv_nsec;
    }

private:
    bool m_isStart;

    static void add(struct timespec & dst, const struct timespec & src) {
        dst.tv_sec += src.tv_sec;
        dst.tv_nsec += src.tv_nsec;
        if (dst.tv_nsec > 1000000000) {
            dst.tv_nsec -= 1000000000;
            dst.tv_sec++;
        }
    }

    static struct timespec diff(const struct timespec & start,
            const struct timespec & end) {
        struct timespec temp;
        if ((end.tv_nsec - start.tv_nsec) < 0) {
            temp.tv_sec = end.tv_sec - start.tv_sec - 1;
            temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
        } else {
            temp.tv_sec = end.tv_sec - start.tv_sec;
            temp.tv_nsec = end.tv_nsec - start.tv_nsec;
        }
        return temp;
    }
    struct timespec m_start;
    struct timespec m_total;
};
#endif
