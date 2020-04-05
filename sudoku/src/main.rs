extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

// Board is a 9 x 9 grid. Should ultimately contain 1 .. 9, each 9 times.
//
// Constraints:
//   1. In each row, each number should appear exactly once.
//   2. In each column, each number should appear exactly once.
//   3. In each 3x3 region (the obvious ones), each number should appear exactly once.
//
// These constraints drive the derivation of values in each of the grid cells.
//   1. The existence of specific values rule out other values in the same row, column, or cell.
//   2. The absence of all but one value from a row, column, or cell determines that value.
//
// You may have to start guessing! Too bad!

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let _timer = ::std::time::Instant::now();

        let text = "53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79";

        // define Sudoku dataflow, return a handle to the start state.
        let mut probe = Handle::new();
        let mut start = worker.dataflow::<usize,_,_>(|scope| {

            // Where the initial values come from.
            //   (val, row, col): means grid[row][col] = val.
            let (start_input, start) = scope.new_collection();

            let board = start.flat_map(|(val, row, col)| {
                let mut result = Vec::new();
                if val != ('.' as u8) {
                    result.push((val - ('0' as u8), row, col));
                }
                else {
                    for v in 1 .. 10 {
                        result.push((v, row, col));
                    }
                }
                result
            });

            let result =
            sudoku(&board)
                .consolidate();

            result
                .map(|(_val, row, col)| (row, col))
                .count()
                .map(|(_row_col, count)| count)
                .consolidate()
                .inspect(|x| println!("Final counts: {:?}", x))
                .probe_with(&mut probe);

            start_input
        });

        for (count, val) in text.bytes().enumerate() {
            let row = 1 + (count as u8 / 9);
            let col = 1 + (count as u8 % 9);
            start.insert((val, row, col));
        }

        start.advance_to(1);
        start.flush();

        while probe.less_than(start.time()) {
            worker.step();
        }

        println!("{:?}\tRound 0 complete", _timer.elapsed());

        for (count, val) in text.bytes().enumerate() {
            let row = 1 + (count as u8 / 9);
            let col = 1 + (count as u8 % 9);

            start.remove((val, row, col));
            start.insert(('.' as u8, row, col));

            let round = *start.time();
            start.advance_to(round + 1);
            start.flush();

            while probe.less_than(start.time()) {
                worker.step();
            }

            println!("{:?}\tRound {:?} complete", _timer.elapsed(), round);
        }

    }).unwrap();
}

/// From (val, row, col) candidates, restrict based on constraints.
fn sudoku<G: Scope>(
    start: &Collection<G, (u8, u8, u8)>
) -> Collection<G, (u8, u8, u8)>
where G::Timestamp: Lattice+Ord {

    start
        .iterate(|inner| {

            // Identify the row, col pairs with a single candidate value.
            let determined =
            inner.map(|(_val, row, col)| (row, col))
                 .threshold(|_row_col, count| if count == &1 { 1 } else { 0 });

            let determined =
            inner.map(|(val, row, col)| ((row, col), val))
                 .semijoin(&determined);

            let exclusions_row = determined.flat_map(|((row, col), val)| (1 .. 10).filter(move |r| r != &row).map(move |r| ((r, col), val)));
            let exclusions_col = determined.flat_map(|((row, col), val)| (1 .. 10).filter(move |c| c != &col).map(move |c| ((row, c), val)));
            let exclusions_cell = determined.flat_map(|((row, col), val)| {
                let row_off = 1 + 3 * ((row - 1)/3);
                let col_off = 1 + 3 * ((col - 1)/3);
                (row_off .. row_off+3)
                    .flat_map(move |r| (col_off .. col_off+3).map(move |c| (r,c)))
                    .filter(move |r_c| r_c != &(row, col))
                    .map(move |(r,c)| ((r,c),val))
            });

            exclusions_row
                .concat(&exclusions_col)
                .concat(&exclusions_cell)
                .map(|((row, col), val)| (val, row, col))
                .negate()
                .concat(&start.enter(&inner.scope()))
                .threshold(|_row_col, count| if count == &1 { 1 } else { 0 })

        })

}
