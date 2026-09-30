use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};
type DynFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

static WAKE_TIMES: Mutex<BTreeMap<Instant, Vec<Waker>>> = Mutex::new(BTreeMap::new());
static NEW_TASKS: Mutex<Vec<DynFuture>> = Mutex::new(Vec::new());

struct Foo {
    n: u64,
    started: bool,
    sleep: Pin<Box<Sleep>>,
}

impl Future for Foo {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            println!("Start: {}", self.n);
            self.started = true;
        }

        if self.sleep.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }

        println!("End: {}", self.n);
        Poll::Ready(())
    }
}

struct Sleep {
    wake_time: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.wake_time {
            Poll::Ready(())
        } else {
            let mut wake_times = WAKE_TIMES.lock().unwrap();
            let wakers_vec = wake_times.entry(self.wake_time).or_default();
            wakers_vec.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct JoinAll<F> {
    futures: Vec<Pin<Box<F>>>,
}

impl<F: Future> Future for JoinAll<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let is_pending = |future: &mut Pin<Box<F>>| future.as_mut().poll(cx).is_pending();
        self.futures.retain_mut(is_pending);

        if self.futures.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn foo(n: u64) -> Foo {
    let duration = Duration::from_millis(n * 100);
    let sleep = Box::pin(sleep(duration));
    Foo {
        n,
        started: false,
        sleep,
    }
}

fn sleep(duration: Duration) -> Sleep {
    let wake_time = Instant::now() + duration;
    Sleep { wake_time }
}

fn join_all<F: Future>(futures: Vec<F>) -> JoinAll<F> {
    JoinAll {
        futures: futures.into_iter().map(Box::pin).collect(),
    }
}

fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) {
    NEW_TASKS.lock().unwrap().push(Box::pin(future));
}

fn main() {
    let k = 10;

    let mut tasks: Vec<DynFuture> = Vec::new();
    for n in 1..=k {
        tasks.push(Box::pin(foo(n)));
    }

    // let mut joined_future = Box::pin(join_all(futures));

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    // while joined_future.as_mut().poll(&mut cx).is_pending() {
    //     // println!("pending");
    // }

    loop {
        let is_pending = |task: &mut DynFuture| task.as_mut().poll(&mut cx).is_pending();
        tasks.retain_mut(is_pending);

        if tasks.is_empty() {
            break;
        }

        let mut wake_times = WAKE_TIMES.lock().unwrap();
        let next_wake = wake_times.keys().next().expect("sleep forever?");
        thread::sleep(next_wake.saturating_duration_since(Instant::now()));

        while let Some(entry) = wake_times.first_entry()
            && *entry.key() <= Instant::now()
        {
            entry.remove().into_iter().for_each(Waker::wake);
        }
    }
}
