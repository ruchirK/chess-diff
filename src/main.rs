use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::io;

use shakmaty::fen::Fen;
use shakmaty::{CastlingSide, Chess, Color, File, Move, Position, Role, Square};

use pgn_reader::{BufferedReader, Outcome, RawHeader, SanPlus, Skip, Visitor};

#[derive(Debug)]
struct FullPiece {
    pub color: Color,
    pub role: Role,
    pub starting_file: Option<File>,
}

impl fmt::Display for FullPiece {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let color = match self.color {
            Color::Black => "Black",
            Color::White => "White",
        };

        let role = match self.role {
            Role::Bishop => "Bishop",
            Role::King => "King",
            Role::Knight => "Knight",
            Role::Pawn => "Pawn",
            Role::Queen => "Queen",
            Role::Rook => "Rook",
        };

        write!(f, "{}-{}", color, role)?;

        if let Some(file) = self.starting_file {
            write!(f, "-{}", file.char().to_uppercase())?;
        };

        Ok(())
    }
}

struct LastPosition {
    pos: Chess,
    pieces: BTreeMap<Square, FullPiece>,
    moves: usize,
    halfmoves: usize,
}

impl LastPosition {
    fn new() -> LastPosition {
        LastPosition {
            pos: Chess::default(),
            pieces: BTreeMap::new(),
            moves: 0,
            halfmoves: 0,
        }
    }
}

impl Visitor for LastPosition {
    type Result = ();

    fn begin_game(&mut self) {
        self.moves = 0;
        self.pieces.clear();
        self.pos = Chess::default();

        self.pieces.insert(
            Square::A1,
            FullPiece {
                color: Color::White,
                role: Role::Rook,
                starting_file: Some(File::A),
            },
        );
        self.pieces.insert(
            Square::B1,
            FullPiece {
                color: Color::White,
                role: Role::Knight,
                starting_file: Some(File::B),
            },
        );
        self.pieces.insert(
            Square::C1,
            FullPiece {
                color: Color::White,
                role: Role::Bishop,
                starting_file: Some(File::C),
            },
        );
        self.pieces.insert(
            Square::D1,
            FullPiece {
                color: Color::White,
                role: Role::Queen,
                starting_file: Some(File::D),
            },
        );
        self.pieces.insert(
            Square::E1,
            FullPiece {
                color: Color::White,
                role: Role::King,
                starting_file: Some(File::E),
            },
        );
        self.pieces.insert(
            Square::F1,
            FullPiece {
                color: Color::White,
                role: Role::Bishop,
                starting_file: Some(File::F),
            },
        );
        self.pieces.insert(
            Square::G1,
            FullPiece {
                color: Color::White,
                role: Role::Knight,
                starting_file: Some(File::G),
            },
        );
        self.pieces.insert(
            Square::H1,
            FullPiece {
                color: Color::White,
                role: Role::Rook,
                starting_file: Some(File::H),
            },
        );

        self.pieces.insert(
            Square::A2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::A),
            },
        );
        self.pieces.insert(
            Square::B2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::B),
            },
        );
        self.pieces.insert(
            Square::C2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::C),
            },
        );
        self.pieces.insert(
            Square::D2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::D),
            },
        );
        self.pieces.insert(
            Square::E2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::E),
            },
        );
        self.pieces.insert(
            Square::F2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::F),
            },
        );
        self.pieces.insert(
            Square::G2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::G),
            },
        );
        self.pieces.insert(
            Square::H2,
            FullPiece {
                color: Color::White,
                role: Role::Pawn,
                starting_file: Some(File::H),
            },
        );

        self.pieces.insert(
            Square::A8,
            FullPiece {
                color: Color::Black,
                role: Role::Rook,
                starting_file: Some(File::A),
            },
        );
        self.pieces.insert(
            Square::B8,
            FullPiece {
                color: Color::Black,
                role: Role::Knight,
                starting_file: Some(File::B),
            },
        );
        self.pieces.insert(
            Square::C8,
            FullPiece {
                color: Color::Black,
                role: Role::Bishop,
                starting_file: Some(File::C),
            },
        );
        self.pieces.insert(
            Square::D8,
            FullPiece {
                color: Color::Black,
                role: Role::Queen,
                starting_file: Some(File::D),
            },
        );
        self.pieces.insert(
            Square::E8,
            FullPiece {
                color: Color::Black,
                role: Role::King,
                starting_file: Some(File::E),
            },
        );
        self.pieces.insert(
            Square::F8,
            FullPiece {
                color: Color::Black,
                role: Role::Bishop,
                starting_file: Some(File::F),
            },
        );
        self.pieces.insert(
            Square::G8,
            FullPiece {
                color: Color::Black,
                role: Role::Knight,
                starting_file: Some(File::G),
            },
        );
        self.pieces.insert(
            Square::H8,
            FullPiece {
                color: Color::Black,
                role: Role::Rook,
                starting_file: Some(File::H),
            },
        );

        self.pieces.insert(
            Square::A7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::A),
            },
        );
        self.pieces.insert(
            Square::B7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::B),
            },
        );
        self.pieces.insert(
            Square::C7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::C),
            },
        );
        self.pieces.insert(
            Square::D7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::D),
            },
        );
        self.pieces.insert(
            Square::E7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::E),
            },
        );
        self.pieces.insert(
            Square::F7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::F),
            },
        );
        self.pieces.insert(
            Square::G7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::G),
            },
        );
        self.pieces.insert(
            Square::H7,
            FullPiece {
                color: Color::Black,
                role: Role::Pawn,
                starting_file: Some(File::H),
            },
        );
    }

    fn header(&mut self, key: &[u8], value: RawHeader<'_>) {
        // Support games from a non-standard starting position.
        if key == b"FEN" {
            let pos = Fen::from_ascii(value.as_bytes())
                .ok()
                .and_then(|f| f.position().ok());

            if let Some(pos) = pos {
                self.pos = pos;
            }
            panic!();
        }
    }

    fn begin_variation(&mut self) -> Skip {
        Skip(true) // stay in the mainline
    }

    fn san(&mut self, san_plus: SanPlus) {
        if let Ok(m) = san_plus.san.to_move(&self.pos) {
            self.halfmoves += 1;

            if self.halfmoves % 2 == 1 {
                self.moves += 1;
            }

            match &m {
                Move::Normal {
                    role,
                    from,
                    capture,
                    to,
                    promotion,
                } => {
                    let from_piece = self
                        .pieces
                        .remove(from)
                        .expect("source piece expected to exist");
                    assert!(from_piece.role == *role);

                    if let Some(capture) = capture {
                        let to_piece = self
                            .pieces
                            .remove(to)
                            .expect("dest piece expected to exist");
                        assert!(to_piece.role == *capture);
                        assert!(from_piece.color != to_piece.color);
                        println!("{},captured,{},{},{}", to_piece, to, self.moves, from_piece);
                    } else {
                        assert!(!self.pieces.contains_key(to));
                    }

                    if let Some(promotion) = promotion {
                        println!("{},promoted,{},{},null", from_piece, to, self.moves);
                        let new_piece = FullPiece {
                            color: from_piece.color,
                            role: *promotion,
                            starting_file: None,
                        };

                        self.pieces.insert(*to, new_piece);
                    } else {
                        self.pieces.insert(*to, from_piece);
                    }
                }
                Move::Castle { king, rook } => {
                    let side = CastlingSide::from_queen_side(rook < king);
                    let rook_piece = self.pieces.remove(rook).expect("expected rook here");
                    assert!(rook_piece.role == Role::Rook);
                    let king_piece = self.pieces.remove(king).expect("expected king here");
                    assert!(king_piece.role == Role::King);
                    assert!(king_piece.color == rook_piece.color);

                    let rook_square = side.rook_to(rook_piece.color);
                    let king_square = side.king_to(king_piece.color);
                    assert!(!self.pieces.contains_key(&king_square));
                    assert!(!self.pieces.contains_key(&rook_square));

                    self.pieces.insert(rook_square, rook_piece);
                    self.pieces.insert(king_square, king_piece);
                }
                Move::EnPassant { from, to } => {
                    let captured_square = Square::from_coords(to.file(), from.rank());
                    let captured_pawn = self
                        .pieces
                        .remove(&captured_square)
                        .expect("expected captured pawn here");
                    assert!(captured_pawn.role == Role::Pawn);
                    let pawn = self.pieces.remove(from).expect("expected a pawn at from");
                    assert!(pawn.role == Role::Pawn);
                    assert!(pawn.color != captured_pawn.color);
                    assert!(!self.pieces.contains_key(to));
                    println!(
                        "{},captured,{},{},{}",
                        captured_pawn, captured_square, self.moves, pawn
                    );
                    self.pieces.insert(*to, pawn);
                }
                _ => panic!(),
            }

            self.pos.play_unchecked(&m);
        }
    }

    fn outcome(&mut self, outcome: Option<Outcome>) {
        let winner = match outcome {
            Some(o) => {
                if let Outcome::Decisive { winner } = o {
                    Some(winner)
                } else {
                    None
                }
            }
            None => None,
        };
        for (key, val) in self.pieces.iter() {
            if val.role == Role::King && winner.is_some() && val.color != winner.unwrap() {
                println!("{},captured,{},{},null", val, key, self.moves);
            } else {
                println!("{},survived,{},{},null", val, key, self.moves);
            }
        }
    }

    fn end_game(&mut self) -> Self::Result {
        ()
    }
}

fn main() -> io::Result<()> {
    for arg in env::args().skip(1) {
        eprintln!("{}", arg);
        let file = std::fs::File::open(&arg).expect("fopen");

        let uncompressed: Box<dyn io::Read> = if arg.ends_with(".bz2") {
            Box::new(bzip2::read::BzDecoder::new(file))
        } else {
            Box::new(file)
        };

        let mut reader = BufferedReader::new(uncompressed);
        let mut visitor = LastPosition::new();
        reader.read_all(&mut visitor)?;
    }

    Ok(())
}
