pub trait FromMatrix<T> {
    fn from_matrix(value: T) -> Self;
}

pub trait FromChat<T> {
    fn from_chat(value: T) -> Self;
}

pub trait IntoMatrix<T> {
    fn into_matrix(self) -> T;
}

pub trait IntoChat<T> {
    fn into_chat(self) -> T;
}

impl<T, U> IntoMatrix<U> for T
where
    U: FromMatrix<T>,
{
    fn into_matrix(self) -> U {
        U::from_matrix(self)
    }
}

impl<T, U> IntoChat<U> for T
where
    U: FromChat<T>,
{
    fn into_chat(self) -> U {
        U::from_chat(self)
    }
}
